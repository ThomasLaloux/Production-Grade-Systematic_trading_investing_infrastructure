"""
Live Trading Engine Module
===========================
Stateless live trading engine — main orchestrator.

Architecture:
    - Periodic bar-close checks at timeframe boundaries
    - Full indicator recomputation each cycle (stateless)
    - SL/TP orders delegated to broker (intrabar execution)
    - Position reconciliation from broker state after each cycle
    - Rolling DataFrame capped at max(history_window, strategy warmup)

High-Level Flow (Section 8):
    8a. Load strategy model (prod YAML + optim)
    8b. Validate/update historical data
    8c. Compute regime (if needed)
    8d. Start live loop:
        - Wait for bar close
        - Check kill switch status
        - Check market hours
        - Check heartbeat / connectivity
        - Append new bar to history
        - Compute regime (if needed)
        - Recompute indicators (stateless)
        - Evaluate signal on latest bar
        - Pre-trade risk checks
        - Execute order if signal (idempotent, with execution monitoring)
        - Track slippage
        - Reconcile positions with broker
        - Log / track timing
    8e. Shutdown

P1.2 Additions:
    - PreTradeRiskCheck: gate between signal and order execution
    - MarketHoursFilter: prevent trading outside market hours
    - SlippageTracker: track actual vs expected fill prices
    - HeartbeatMonitor: proactive connectivity monitoring

P2.1 Additions:
    - AuditTrail: append-only audit trail (CSV buffer + daily parquet)
    - SpreadFilter: skip signals when bid-ask spread too wide
    - OrderExecutor: partial fill handling (re-entry for unfilled qty)

P2.2 Additions:
    - KillSwitch: emergency flatten (close all, cancel all, halt engine)
    - ExecutionQualityMonitor: fill latency, rejection rate, requote freq,
      slippage by time-of-day and volatility regime
    - Idempotent order submission: deterministic client_order_id from
      signal context prevents duplicate orders on retries

P3.1 Additions:
    - Paper trading mode: _paper_trade flag routes all orders through
      PaperBroker (simulated fill engine) instead of the real broker.
      Uses the same code path — validates the full live pipeline
      without capital at risk. Fill at current price + slippage.

Classes:
    LiveTradingEngine:
        - run: main loop
        - shutdown: clean shutdown

Usage:
    from live import LiveTradingEngine, LiveConfigurator
    config = LiveConfigurator(_path="live/live_params.yaml")

    engine = LiveTradingEngine(
        _broker=broker, _strategy=strategy,
        _data_manager=data_manager,
        _data_configurator=data_configurator,
        _brokers_configurator=brokers_configurator,
        _symbol=symbol, _broker_symbol=broker_symbol,
        _timeframe="M15",
        _config=config,
        _risk_pct=0.005,
        _max_trades_per_day=0,
        _contract_size=100,
    )
    engine.run()
"""

import logging
import signal
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import pandas as pd
import numpy as np

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from strategies import StrategyBase, Signal, TradeDirection
from core.data_types import OrderSide

from .live_configurator import LiveConfigurator
from .bar_timer import BarTimer
from .position_sizer import PositionSizer
from .order_executor import OrderExecutor
from .state_reconciler import StateReconciler
from .data_validator import DataValidator
from .risk_checks import PreTradeRiskCheck
from .market_hours import MarketHoursFilter, MarketStatus
from .slippage_tracker import SlippageTracker
from .heartbeat import HeartbeatMonitor
from .spread_filter import SpreadFilter
from .audit_trail import AuditTrail
from .kill_switch import KillSwitch
from .execution_monitor import ExecutionQualityMonitor
from .paper_broker import PaperBroker

# P7.2: Calendar news — dynamic holidays + news blackout filter
try:
    from data import (
        FinnhubCalendarManager,
        InstrumentCurrencyMap,
        NewsFilter,
    )
    CALENDAR_NEWS_AVAILABLE = True
except ImportError:
    CALENDAR_NEWS_AVAILABLE = False

logger = logging.getLogger(__name__)


class LiveTradingEngine:
    """
    Stateless live trading engine.

    Architecture:
        - Periodic bar-close checks at timeframe boundaries
        - Full indicator recomputation each cycle (stateless)
        - SL/TP orders delegated to broker (intrabar execution)
        - Position reconciliation from broker state after each cycle
        - Pre-trade risk checks (circuit breakers)
        - Market hours filtering
        - Slippage tracking
        - Heartbeat / connectivity monitoring
        - Kill switch / emergency flatten (P2.2)
        - Execution quality monitoring (P2.2)
        - Idempotent order submission (P2.2)
        - Paper trading mode — PaperBroker drop-in (P3.1)
    """

    def __init__(
        self,
        _broker: 'BrokerBase',
        _strategy: StrategyBase,
        _data_manager: 'DataManager',
        _data_configurator: 'DataConfigurator',
        _brokers_configurator: 'BrokersConfigurator',
        _symbol: str,
        _broker_symbol: str,
        _timeframe: str,
        _config: LiveConfigurator,
        _risk_pct: float,
        _max_trades_per_day: int,
        _contract_size: float,
        _finnhub_api_key: Optional[str] = None,
        _calendar_db_dir: str = "data/calendar_news_db",
        _instruments_path: str = "data/instruments.yaml",
    ):
        """
        Initialize LiveTradingEngine.

        All inputs are explicit — no hidden defaults.

        Args:
            _broker: Connected broker instance.
            _strategy: Initialized strategy instance (mode='live').
            _data_manager: DataManager for data sync and loading.
            _data_configurator: DataConfigurator for instrument metadata.
            _brokers_configurator: BrokersConfigurator for broker settings.
            _symbol: Standard symbol (e.g. "XAUUSD").
            _broker_symbol: Broker-specific symbol (e.g. "XAUUSDp").
            _timeframe: Timeframe string (e.g. "M15").
            _config: LiveConfigurator with engine/risk/logging settings.
            _risk_pct: Risk per trade as fraction of equity (from strategy YAML).
            _max_trades_per_day: Max trades per day (from strategy YAML). 0 = unlimited.
            _contract_size: Contract size for the instrument.
            _finnhub_api_key: Finnhub API key for calendar data.
            _calendar_db_dir: Directory for calendar DB storage.
            _instruments_path: Path to instruments.yaml.
        """
        # --- Core components ---
        self._broker = _broker
        self._strategy = _strategy
        self._data_manager = _data_manager
        self._data_configurator = _data_configurator
        self._brokers_configurator = _brokers_configurator
        self._symbol = _symbol
        self._broker_symbol = _broker_symbol
        self._timeframe = _timeframe
        self._risk_pct = _risk_pct
        self._max_trades_per_day = _max_trades_per_day
        self._contract_size = _contract_size

        # --- Load config sections ---
        engine_settings = _config.get_engine_settings()
        circuit_breakers = _config.get_circuit_breakers()
        data_validation = _config.get_data_validation()
        market_hours_config = _config.get_market_hours()
        heartbeat_config = _config.get_heartbeat()
        slippage_config = _config.get_slippage()
        logging_config = _config.get_logging()
        spread_filter_config = _config.get_spread_filter()
        audit_trail_config = _config.get_audit_trail()
        partial_fills_config = _config.get_partial_fills()
        kill_switch_config = _config.get_kill_switch()
        idempotency_config = _config.get_idempotency()
        execution_monitor_config = _config.get_execution_monitor()
        paper_trade_config = _config.get_paper_trade()

        # --- P3.1: Paper trading mode ---
        # When enabled, wrap the real broker with PaperBroker.
        # All downstream code uses self._broker and gets the PaperBroker,
        # which delegates data calls (get_server_time, get_tick_data,
        # get_instrument_metadata) to the real broker but intercepts
        # all order/position/account operations with simulated logic.
        self._paper_trade = paper_trade_config['enabled']
        self._paper_broker: Optional[PaperBroker] = None

        if self._paper_trade:
            self._paper_broker = PaperBroker(
                _real_broker=self._broker,
                _data_configurator=_data_configurator,
                _initial_balance=paper_trade_config['initial_balance'],
                _slippage_pips=paper_trade_config['slippage_pips'],
                _commission_per_lot=paper_trade_config['commission_per_lot'],
            )
            # Swap the broker reference — all downstream components will
            # use PaperBroker transparently via the same interface.
            self._broker = self._paper_broker
            logger.info(
                f"LiveEngine: PAPER TRADING MODE — "
                f"balance={paper_trade_config['initial_balance']:.2f}, "
                f"slippage={paper_trade_config['slippage_pips']} pips, "
                f"commission={paper_trade_config['commission_per_lot']}/lot"
            )

        self._history_window = engine_settings['history_window']
        self._poll_interval_seconds = engine_settings['poll_interval_seconds']
        self._max_daily_dd_pct = circuit_breakers['max_daily_dd_pct']
        self._max_total_dd_pct = circuit_breakers['max_total_dd_pct']
        self._max_position_lots = circuit_breakers['max_position_lots']
        self._print_timing = logging_config['print_timing']
        self._market_hours_enabled = market_hours_config['enabled']
        self._heartbeat_enabled = heartbeat_config['enabled']
        self._slippage_enabled = slippage_config['enabled']
        self._spread_filter_enabled = spread_filter_config['enabled']
        self._audit_trail_enabled = audit_trail_config['enabled']
        self._partial_fill_enabled = partial_fills_config['enabled']
        self._kill_switch_enabled = kill_switch_config['enabled']
        self._idempotency_enabled = idempotency_config['enabled']
        self._execution_monitor_enabled = execution_monitor_config['enabled']

        # Ensure history_window >= strategy warmup
        warmup = self._strategy.get_warmup_period()
        if self._history_window < warmup:
            logger.warning(
                f"LiveEngine: history_window ({self._history_window}) < "
                f"strategy warmup ({warmup}). Increasing to {warmup}."
            )
            self._history_window = warmup

        # --- Get instrument metadata ---
        broker_name = (
            _broker.broker_name
            if hasattr(_broker, 'broker_name')
            else _brokers_configurator.list_brokers()[0]
        )
        self._broker_name = broker_name
        self._instrument = _data_configurator.get_instrument(
            _broker_symbol, _broker=broker_name,
        )

        # --- P1.1 Sub-components ---
        self._bar_timer = BarTimer(
            _broker=_broker,
            _symbol=_broker_symbol,
            _timeframe=_timeframe,
            _poll_interval_seconds=self._poll_interval_seconds,
        )
        self._position_sizer = PositionSizer(
            _instrument_metadata=self._instrument,
        )

        # --- P2.2 Sub-components (initialized before OrderExecutor) ---

        # Execution quality monitor
        self._execution_monitor: Optional[ExecutionQualityMonitor] = None
        if self._execution_monitor_enabled:
            self._execution_monitor = ExecutionQualityMonitor(
                _broker_name=broker_name,
                _output_dir=execution_monitor_config['output_dir'],
            )

        self._order_executor = OrderExecutor(
            _broker=_broker,
            _partial_fill_enabled=self._partial_fill_enabled,
            _max_partial_fill_retries=partial_fills_config['max_retries'],
            _idempotency_enabled=self._idempotency_enabled,
            _execution_monitor=self._execution_monitor,
        )
        self._state_reconciler = StateReconciler(_broker=_broker)
        self._data_validator = DataValidator(
            _data_manager=_data_manager,
            _max_gap_bars=data_validation['max_gap_bars'],
            _forward_fill_gaps=data_validation['forward_fill_gaps'],
        )

        # --- P1.2 Sub-components ---

        # Pre-trade risk checks
        self._risk_check = PreTradeRiskCheck(
            _max_daily_dd_pct=self._max_daily_dd_pct,
            _max_total_dd_pct=self._max_total_dd_pct,
            _max_trades_per_day=self._max_trades_per_day,
            _max_position_lots=self._max_position_lots,
        )

        # --- P7.2: Calendar news, holidays, and news filter ---
        # These are independent of market_hours_enabled — the news filter
        # can suppress signals even if market hours filtering is off.
        self._calendar_manager: Optional[Any] = None
        self._currency_map: Optional[Any] = None
        self._news_filter: Optional[Any] = None

        # Read news filter config from the strategy instance
        self._news_filter_mode = _strategy.news_filter_mode
        self._news_filter_before_minutes = _strategy.news_filter_before_minutes
        self._news_filter_after_minutes = _strategy.news_filter_after_minutes
        self._inverse_news_filter = _strategy.inverse_news_filter

        # Holidays loaded from calendar DB (passed to MarketHoursFilter below)
        _calendar_holidays: list = []

        if CALENDAR_NEWS_AVAILABLE and _finnhub_api_key:
            try:
                self._currency_map = InstrumentCurrencyMap(
                    _instruments_path=_instruments_path,
                )
                self._calendar_manager = FinnhubCalendarManager(
                    _api_key=_finnhub_api_key,
                    _db_dir=_calendar_db_dir,
                )
                # Run update at startup (backfill if needed, then daily)
                logger.info("LiveEngine: updating calendar DB at startup...")
                self._calendar_manager.update()

                # Load holidays for current year from calendar DB
                import datetime as dt_module
                current_year = dt_module.date.today().year
                _calendar_holidays = self._calendar_manager.get_holiday_dates(
                    _year=current_year,
                )
                logger.info(
                    f"LiveEngine: loaded {len(_calendar_holidays)} holidays "
                    f"from calendar DB for {current_year}"
                )

                # Build news filter if strategy has it enabled
                if self._news_filter_mode != "disabled":
                    self._news_filter = NewsFilter(
                        _calendar_manager=self._calendar_manager,
                        _currency_map=self._currency_map,
                    )
                    logger.info(
                        f"LiveEngine: NewsFilter enabled — "
                        f"mode={self._news_filter_mode}, "
                        f"before={self._news_filter_before_minutes}min, "
                        f"after={self._news_filter_after_minutes}min, "
                        f"inverse={self._inverse_news_filter}"
                    )
            except Exception as e:
                logger.warning(
                    f"LiveEngine: calendar_news init failed — {e}. "
                    f"Falling back to empty holidays."
                )
        elif not CALENDAR_NEWS_AVAILABLE:
            logger.info(
                "LiveEngine: calendar_news module not available. "
                "Holidays and news filter disabled."
            )
        elif not _finnhub_api_key:
            logger.info(
                "LiveEngine: no Finnhub API key provided. "
                "Calendar news disabled."
            )

        # Market hours filter
        self._market_hours_filter: Optional[MarketHoursFilter] = None
        if self._market_hours_enabled:
            broker_cfg = _brokers_configurator.get_broker_config(broker_name)
            mh_data = broker_cfg.get('market_hours', {})
            asset_class = market_hours_config['asset_class']
            mh_timezone = mh_data.get('timezone', 'UTC')

            self._market_hours_filter = MarketHoursFilter(
                _market_hours_config=mh_data,
                _asset_class=asset_class,
                _holidays=_calendar_holidays,
                _timezone=mh_timezone,
            )

        # Slippage tracker
        self._slippage_tracker: Optional[SlippageTracker] = None
        if self._slippage_enabled:
            self._slippage_tracker = SlippageTracker()

        # Heartbeat monitor
        self._heartbeat: Optional[HeartbeatMonitor] = None
        if self._heartbeat_enabled:
            self._heartbeat = HeartbeatMonitor(
                _broker=_broker,
                _symbol=_broker_symbol,
                _check_interval_seconds=heartbeat_config['check_interval_seconds'],
                _stale_data_threshold_seconds=heartbeat_config['stale_data_threshold_seconds'],
                _market_hours_filter=self._market_hours_filter,
            )

        # --- P2.1 Sub-components ---

        # Spread filter
        self._spread_filter: Optional[SpreadFilter] = None
        if self._spread_filter_enabled:
            pip_size = self._instrument.pip_size if self._instrument else 0.01
            self._spread_filter = SpreadFilter(
                _broker=_broker,
                _max_spread_pips=spread_filter_config['max_spread_pips'],
                _pip_size=pip_size,
            )

        # Audit trail / audit trail
        self._audit_trail: Optional[AuditTrail] = None
        if self._audit_trail_enabled:
            self._audit_trail = AuditTrail(
                _audit_dir=audit_trail_config['audit_dir'],
                _strategy=self._strategy.name,
                _symbol=_broker_symbol,
                _timeframe=_timeframe,
                _flush_interval_records=audit_trail_config['flush_interval_records'],
            )

        # --- P2.2 Sub-components (continued) ---

        # Kill switch / emergency flatten
        self._kill_switch: Optional[KillSwitch] = None
        if self._kill_switch_enabled:
            self._kill_switch = KillSwitch(
                _broker=_broker,
                _sentinel_path=kill_switch_config['sentinel_path'],
                _poll_interval_seconds=kill_switch_config['poll_interval_seconds'],
            )
            # Wire kill switch to halt engine
            self._kill_switch.set_engine_halt_callback(
                lambda: setattr(self, '_running', False)
            )
            # Wire kill switch to trade journal (if enabled)
            if self._audit_trail is not None:
                self._kill_switch.set_audit_trail(self._audit_trail)

        # --- State ---
        self._running = False
        self._data: Optional[pd.DataFrame] = None
        self._cycle_count = 0
        self._daily_trade_count = 0
        self._daily_start_equity: Optional[float] = None
        self._initial_equity: Optional[float] = None
        self._current_trading_day: Optional[datetime] = None
        self._strategy_name = self._strategy.name

        # --- Shutdown hook ---
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, _signum: int, _frame: Any) -> None:
        """Handle SIGINT/SIGTERM for clean shutdown."""
        logger.info(f"LiveEngine: received signal {_signum}, shutting down...")
        print(f"\nLiveEngine: shutdown signal received. Stopping...")
        self._running = False

    def run(self) -> None:
        """
        Main live trading loop.

        Flow per cycle:
            1. Wait for bar completion (BarTimer)
            2. Check market hours (MarketHoursFilter)
            3. Check heartbeat / connectivity (HeartbeatMonitor)
            4. Fetch latest bar from broker / update parquet
            5. Rebuild full DataFrame (stateless, rolling window)
            6. Recompute regime + indicators
            7. Evaluate signal on latest completed bar
            8. Pre-trade risk checks (PreTradeRiskCheck)
            9. If signal and no position: execute order
            10. Track slippage (SlippageTracker)
            11. If position exists: check exit conditions (beyond SL/TP)
            12. Reconcile state with broker
            13. Log cycle
        """
        self._running = True
        logger.info(
            f"LiveEngine: starting for {self._broker_symbol} on {self._timeframe}, "
            f"strategy={self._strategy_name}, "
            f"paper_trade={self._paper_trade}, "
            f"history_window={self._history_window}, "
            f"risk_pct={self._risk_pct}, "
            f"max_trades_per_day={self._max_trades_per_day}"
        )
        print(f"\n{'='*60}")
        print(f"LIVE TRADING ENGINE — {self._broker_symbol} {self._timeframe}")
        if self._paper_trade:
            print(f"*** PAPER TRADING MODE ON ***")
        print(f"Strategy: {self._strategy_name}")
        print(f"Risk per trade: {self._risk_pct*100:.2f}%")
        print(f"Max trades/day: {self._max_trades_per_day if self._max_trades_per_day > 0 else 'unlimited'}")
        print(f"History window: {self._history_window} bars")
        print(f"Paper trading: {'ON' if self._paper_trade else 'OFF'}")
        print(f"Market hours filter: {'ON' if self._market_hours_enabled else 'OFF'}")
        print(f"Heartbeat monitor: {'ON' if self._heartbeat_enabled else 'OFF'}")
        print(f"Slippage tracking: {'ON' if self._slippage_enabled else 'OFF'}")
        print(f"Spread filter: {'ON' if self._spread_filter_enabled else 'OFF'}")
        print(f"Audit trail: {'ON' if self._audit_trail_enabled else 'OFF'}")
        print(f"Partial fill handling: {'ON' if self._partial_fill_enabled else 'OFF'}")
        print(f"Kill switch: {'ON' if self._kill_switch_enabled else 'OFF'}")
        print(f"Idempotent orders: {'ON' if self._idempotency_enabled else 'OFF'}")
        print(f"Execution monitor: {'ON' if self._execution_monitor_enabled else 'OFF'}")
        nf_mode = getattr(self, '_news_filter_mode', 'disabled')
        nf_inverse = getattr(self, '_inverse_news_filter', False)
        print(f"News filter: {nf_mode}", end="")
        if nf_mode != "disabled":
            print(f" (before={self._news_filter_before_minutes}min, "
                  f"after={self._news_filter_after_minutes}min"
                  f"{', INVERSE' if nf_inverse else ''})")
        else:
            print()
        print(f"{'='*60}\n")

        # --- 8a. Validate and load historical data ---
        print("LiveEngine: validating historical data...")
        warmup = self._strategy.get_warmup_period()
        self._data = self._data_validator.validate(
            _source_name=self._broker_name,
            _symbol=self._broker_symbol,
            _timeframe=self._timeframe,
            _warmup_bars=warmup,
        )

        # Trim to rolling window
        self._data = self._trim_to_window(self._data)

        # --- Get initial account state ---
        account = self._state_reconciler.get_account_state()
        self._initial_equity = account.get('equity', account.get('balance', 0))
        self._daily_start_equity = self._initial_equity
        self._current_trading_day = datetime.now(timezone.utc).date()

        print(f"LiveEngine: initial equity={self._initial_equity:.2f}, data={len(self._data)} bars\n")

        # --- Log ENGINE START to journal (P2.1) ---
        if self._audit_trail is not None:
            self._audit_trail.log(
                _event_type="ENGINE",
                _reason="START",
                _equity=self._initial_equity,
                _balance=self._initial_equity,
                _details={
                    "strategy": self._strategy_name,
                    "symbol": self._broker_symbol,
                    "timeframe": self._timeframe,
                    "history_window": self._history_window,
                    "risk_pct": self._risk_pct,
                    "max_trades_per_day": self._max_trades_per_day,
                    "data_bars": len(self._data),
                    "paper_trade": self._paper_trade,
                },
            )

        # --- Start heartbeat monitor ---
        if self._heartbeat is not None:
            self._heartbeat.start()

        # --- P2.2: Start kill switch monitor ---
        if self._kill_switch is not None:
            self._kill_switch.start_monitor()

        # --- 8d. Main live loop ---
        print("LiveEngine: entering live loop. Press Ctrl+C to stop.\n")

        while self._running:
            try:
                self._run_cycle()
            except KeyboardInterrupt:
                break
            except Exception as e:
                logger.error(f"LiveEngine: cycle error — {e}", exc_info=True)
                print(f"  LiveEngine: ERROR cycle error: {e}")
                # Continue running — errors in a single cycle shouldn't halt the engine
                # (Error recovery is out of scope for P1.1, handled in P5)
                time.sleep(self._poll_interval_seconds)

        # --- 8e. Shutdown ---
        self.shutdown()

    def _run_cycle(self) -> None:
        """Execute one bar cycle."""
        cycle_start = time.perf_counter()
        self._cycle_count += 1

        # --- 1. Wait for bar completion (silent — no display) ---
        server_time = self._bar_timer.wait_for_bar_close()
        cycle_ts = server_time.strftime('%Y-%m-%d %H:%M')

        if not self._running:
            return

        # --- P2.2: Check kill switch ---
        if self._kill_switch is not None and self._kill_switch.is_triggered:
            logger.info("LiveEngine: kill switch triggered, stopping engine")
            print(f"{cycle_ts} KillSwitch: engine halted")
            self._running = False
            return

        # --- Reset daily counters if new day ---
        current_day = server_time.date()
        if self._current_trading_day != current_day:
            self._current_trading_day = current_day
            self._daily_trade_count = 0
            account = self._state_reconciler.get_account_state()
            self._daily_start_equity = account.get('equity', account.get('balance', 0))
            logger.info(f"LiveEngine: new trading day {current_day}, "
                        f"start equity: {self._daily_start_equity:.2f}")
            print(f"{cycle_ts} LiveEngine: new day {current_day}, equity={self._daily_start_equity:.2f}")

        # --- 2. Check market hours ---
        t0 = time.perf_counter()
        if self._market_hours_filter is not None:
            if not self._market_hours_filter.is_market_open(_server_time=server_time):
                next_open = self._market_hours_filter.next_open_time(
                    _server_time=server_time,
                )
                logger.info(
                    f"LiveEngine: market closed at {server_time}. "
                    f"Next open: {next_open}"
                )
                print(f"{cycle_ts} MarketHours: CLOSED, next open {next_open}. Skipping.")
                return
        t_market_hours = time.perf_counter() - t0

        # --- 3. Check heartbeat / connectivity ---
        t0 = time.perf_counter()
        if self._heartbeat is not None:
            if not self._heartbeat.is_healthy():
                failures = self._heartbeat.consecutive_failures
                logger.warning(
                    f"LiveEngine: heartbeat unhealthy "
                    f"(consecutive_failures={failures}). Skipping cycle."
                )
                print(f"{cycle_ts} HeartbeatMonitor: UNHEALTHY "
                      f"(failures={failures}). Skipping cycle.")
                return
        t_heartbeat = time.perf_counter() - t0

        # --- 4. Fetch latest bar and update data ---
        t0 = time.perf_counter()
        self._fetch_and_append_bar()
        t_fetch = time.perf_counter() - t0

        # --- 5. Regime computation (handled by strategy.initialize) ---
        # Phase 8.4: The strategy computes its own regime during initialize().
        # No external regime model needed — step 6 (initialize) handles it.
        t_regime = 0.0

        # --- 6. Recompute indicators (stateless) ---
        t0 = time.perf_counter()
        self._strategy.initialize(self._data)
        t_indicators = time.perf_counter() - t0

        # --- 7. Evaluate signal on latest completed bar ---
        t0 = time.perf_counter()
        live_signals = self._strategy.generate_signals(_mode='live')
        signal_result = live_signals[0] if live_signals else None
        t_signal = time.perf_counter() - t0

        # --- 7b. P7.2: News blackout filter ---
        t0 = time.perf_counter()
        news_blackout = False
        if (
            self._news_filter is not None
            and self._news_filter_mode != "disabled"
        ):
            news_blackout, blackout_events = self._news_filter.is_in_blackout(
                _instrument=self._broker_symbol,
                _broker=self._broker_name,
                _server_time=server_time,
                _before_minutes=self._news_filter_before_minutes,
                _after_minutes=self._news_filter_after_minutes,
                _inverse=self._inverse_news_filter,
            )
            if news_blackout:
                # Label depends on mode: standard blackout vs inverse (outside window)
                label = "INVERSE BLACKOUT" if self._inverse_news_filter else "BLACKOUT"
                event_names = [e["event_title"] for e in blackout_events]
                logger.info(
                    f"LiveEngine: NEWS {label} — {len(blackout_events)} "
                    f"events: {event_names}"
                )
                print(
                    f"{cycle_ts} NewsFilter: {label} "
                    f"({len(blackout_events)} events). "
                    f"Mode={self._news_filter_mode}."
                )
                # Block new entries
                signal_result = None

                # Close positions if mode requires it.
                # Standard mode: close at blackout ENTRY (window start).
                # Inverse mode: close at trading window EXIT (after_minutes reached,
                #   i.e. we've just left the window → blackout started).
                if self._news_filter_mode == "close_positions":
                    positions = self._state_reconciler.get_positions(
                        _symbol=self._broker_symbol,
                        _strategy=self._strategy_name,
                    )
                    if positions:
                        logger.info(
                            f"LiveEngine: news {label.lower()} — closing "
                            f"{len(positions)} position(s)"
                        )
                        print(
                            f"{cycle_ts} NewsFilter: closing "
                            f"{len(positions)} position(s)"
                        )
                        for pos in positions:
                            try:
                                self._broker.close_position(
                                    _symbol=self._broker_symbol,
                                )
                            except Exception as e:
                                logger.error(
                                    f"LiveEngine: failed to close position "
                                    f"during news {label.lower()} — {e}"
                                )

                # Log to audit trail
                if self._audit_trail is not None:
                    self._audit_trail.log(
                        _event_type="NEWS_BLACKOUT",
                        _cycle_number=self._cycle_count,
                        _details={
                            "mode": self._news_filter_mode,
                            "inverse": self._inverse_news_filter,
                            "events": [
                                {
                                    "title": e["event_title"],
                                    "currency": e["currency"],
                                    "time": str(e["datetime_utc"]),
                                }
                                for e in blackout_events
                            ],
                        },
                    )
        t_news_filter = time.perf_counter() - t0

        # --- 8. Execute order if signal (with pre-trade risk checks) ---
        t0 = time.perf_counter()
        if signal_result is not None:
            self._handle_signal(signal_result)
        t_order = time.perf_counter() - t0

        # --- 9. Check exit conditions for existing positions ---
        t0 = time.perf_counter()
        self._check_exit_conditions()
        t_exit = time.perf_counter() - t0

        # --- 10. Reconcile state with broker ---
        t0 = time.perf_counter()
        self._state_reconciler.get_positions(
            _symbol=self._broker_symbol, _strategy=self._strategy_name,
        )
        t_reconcile = time.perf_counter() - t0

        # --- 11. Log cycle ---
        cycle_elapsed = time.perf_counter() - cycle_start

        # --- P2.1: Log cycle to journal ---
        if self._audit_trail is not None:
            account = self._state_reconciler.get_account_state()
            cycle_equity = account.get('equity', account.get('balance', 0))
            cycle_balance = account.get('balance', 0)

            self._audit_trail.log(
                _event_type="CYCLE",
                _cycle_number=self._cycle_count,
                _equity=cycle_equity,
                _balance=cycle_balance,
                _elapsed_ms=cycle_elapsed * 1000,
                _details={
                    "signal": signal_result.direction.name if signal_result else None,
                    "bars": len(self._data) if self._data is not None else 0,
                },
            )

            # Log account state at each cycle
            self._audit_trail.log(
                _event_type="ACCOUNT",
                _cycle_number=self._cycle_count,
                _equity=cycle_equity,
                _balance=cycle_balance,
                _details=account,
            )

        if self._print_timing:
            sig_str = signal_result.direction.name if signal_result else "none"
            print(f"{cycle_ts} Cycle {self._cycle_count}: "
                  f"bars={len(self._data) if self._data is not None else 0}, "
                  f"signal={sig_str}, "
                  f"{cycle_elapsed*1000:.0f}ms "
                  f"(fetch={t_fetch*1000:.0f} ind={t_indicators*1000:.0f} "
                  f"sig={t_signal*1000:.0f} order={t_order*1000:.0f})")

        logger.info(
            f"LiveEngine: cycle {self._cycle_count} complete in "
            f"{cycle_elapsed*1000:.1f}ms "
            f"(mkt_hrs={t_market_hours*1000:.0f}, hb={t_heartbeat*1000:.0f}, "
            f"fetch={t_fetch*1000:.0f}, regime={t_regime*1000:.0f}, "
            f"ind={t_indicators*1000:.0f}, sig={t_signal*1000:.0f}, "
            f"order={t_order*1000:.0f}, exit={t_exit*1000:.0f}, "
            f"recon={t_reconcile*1000:.0f})"
        )

    def _fetch_and_append_bar(self) -> None:
        """
        Fetch latest data and update rolling DataFrame.

        Syncs with broker, resamples to target TF, and trims to rolling window.
        """
        self._data_manager.sync_data(
            _source_name=self._broker_name,
            _symbol=self._broker_symbol,
            _timeframe="M1",
            _run_quality_check=False,
        )

        df_fresh = self._data_manager.get_ohlcv(
            _source_name=self._broker_name,
            _symbol=self._broker_symbol,
            _timeframe=self._timeframe,
            _validate=False,
        )

        if df_fresh is not None and not df_fresh.empty:
            self._data = df_fresh
            self._data = self._trim_to_window(self._data)
        else:
            logger.warning("LiveEngine: no fresh data received from sync")
            print(f"  Data: WARNING no fresh data from sync")

    def _trim_to_window(self, _df: pd.DataFrame) -> pd.DataFrame:
        """Trim DataFrame to rolling window size."""
        if len(_df) > self._history_window:
            return _df.iloc[-self._history_window:].reset_index(drop=True)
        return _df

    def _handle_signal(self, _signal: Signal) -> None:
        """
        Process a trading signal: spread check, risk checks, sizing, execution,
        slippage tracking, and journal logging (P2.1).
        P2.2: generates idempotent client_order_id, passes execution context
        to ExecutionQualityMonitor via OrderExecutor.

        Args:
            _signal: Signal from strategy.
        """
        # --- Log signal to journal (P2.1) ---
        if self._audit_trail is not None:
            self._audit_trail.log(
                _event_type="SIGNAL",
                _cycle_number=self._cycle_count,
                _direction=_signal.direction.name,
                _entry_price=_signal.entry_price,
                _stop_loss=_signal.stop_loss,
                _take_profit=_signal.take_profit,
                _details=_signal.metadata if _signal.metadata else None,
            )

        # Check if position already exists
        has_pos = self._state_reconciler.has_position(
            _symbol=self._broker_symbol, _strategy=self._strategy_name,
        )
        if has_pos:
            logger.info("LiveEngine: signal ignored — position already open")
            print(f"  Signal: {_signal.direction.name} ignored, position open")
            return

        # Validate SL
        if _signal.stop_loss is None:
            logger.warning("LiveEngine: signal has no SL, skipping")
            print(f"  Signal: WARNING no SL, skipping")
            return

        # --- P2.1: Spread filter ---
        if self._spread_filter is not None:
            spread_approved, spread_pips, spread_reason = self._spread_filter.check(
                _symbol=self._broker_symbol,
            )

            # Log spread check to journal
            if self._audit_trail is not None:
                self._audit_trail.log(
                    _event_type="SPREAD_CHECK",
                    _cycle_number=self._cycle_count,
                    _direction=_signal.direction.name,
                    _approved=spread_approved,
                    _reason=spread_reason,
                    _details={"spread_pips": spread_pips},
                )

            if not spread_approved:
                logger.info(
                    f"LiveEngine: signal SKIPPED by spread filter — {spread_reason}"
                )
                return

        # Calculate position size
        account = self._state_reconciler.get_account_state()
        equity = account.get('equity', account.get('balance', 0))

        quantity = self._position_sizer.calculate(
            _equity=equity,
            _risk_pct=self._risk_pct,
            _entry_price=_signal.entry_price,
            _stop_loss=_signal.stop_loss,
        )

        # --- Pre-trade risk checks (P1.2) ---
        approved, reason = self._risk_check.approve(
            _equity=equity,
            _daily_start_equity=self._daily_start_equity if self._daily_start_equity else equity,
            _initial_equity=self._initial_equity if self._initial_equity else equity,
            _daily_trade_count=self._daily_trade_count,
            _quantity=quantity,
        )

        # Log risk check to journal (P2.1)
        if self._audit_trail is not None:
            self._audit_trail.log(
                _event_type="RISK_CHECK",
                _cycle_number=self._cycle_count,
                _direction=_signal.direction.name,
                _quantity=quantity,
                _equity=equity,
                _approved=approved,
                _reason=reason,
            )

        if not approved:
            logger.warning(
                f"LiveEngine: signal REJECTED by risk check — {reason}"
            )
            # If circuit breaker (DD breach), trigger kill switch or halt engine
            if "DD_BREACH" in reason:
                if self._kill_switch is not None:
                    self._kill_switch.activate(_reason=reason)
                else:
                    self._running = False
            return

        # Determine order side
        side = (OrderSide.BUY if _signal.direction == TradeDirection.LONG
                else OrderSide.SELL)

        # --- P2.2: Generate deterministic client_order_id ---
        client_order_id = None
        if self._idempotency_enabled:
            # Use the signal bar timestamp for deterministic ID generation
            signal_ts = ""
            if self._data is not None and len(self._data) > 0:
                signal_ts = str(self._data['timestamp'].iloc[-1])
            client_order_id = OrderExecutor.generate_client_order_id(
                _strategy=self._strategy_name,
                _symbol=self._broker_symbol,
                _timeframe=self._timeframe,
                _signal_timestamp=signal_ts,
                _direction=side.name,
            )

        # --- P2.2: Determine current volatility regime (if available) ---
        volatility_regime = ""
        if (self._data is not None
                and 'regime' in self._data.columns
                and len(self._data) > 0):
            regime_val = self._data['regime'].iloc[-1]
            if regime_val is not None:
                volatility_regime = str(regime_val)

        # Execute order
        tp_str = f"{_signal.take_profit:.5f}" if _signal.take_profit is not None else "none"
        print(f"  Signal: {_signal.direction.name} @ {_signal.entry_price:.5f}, "
              f"SL={_signal.stop_loss:.5f}, TP={tp_str}, qty={quantity:.2f}")

        # Log order submission to journal (P2.1)
        if self._audit_trail is not None:
            self._audit_trail.log(
                _event_type="ORDER",
                _cycle_number=self._cycle_count,
                _direction=_signal.direction.name,
                _entry_price=_signal.entry_price,
                _stop_loss=_signal.stop_loss,
                _take_profit=_signal.take_profit,
                _quantity=quantity,
                _equity=equity,
                _details={
                    "client_order_id": client_order_id,
                },
            )

        try:
            order = self._order_executor.submit_market_order(
                _symbol=self._broker_symbol,
                _side=side,
                _quantity=quantity,
                _stop_loss=_signal.stop_loss,
                _take_profit=_signal.take_profit,
                _strategy=self._strategy_name,
                _client_order_id=client_order_id,
                _expected_price=_signal.entry_price,
                _volatility_regime=volatility_regime,
            )

            # P2.2: order may be None if idempotency check rejected it
            if order is None:
                logger.info(
                    "LiveEngine: order skipped — duplicate client_order_id"
                )
                if self._audit_trail is not None:
                    self._audit_trail.log(
                        _event_type="ENGINE",
                        _cycle_number=self._cycle_count,
                        _reason="ORDER_SKIPPED_DUPLICATE",
                        _direction=_signal.direction.name,
                        _entry_price=_signal.entry_price,
                        _quantity=quantity,
                        _details={
                            "client_order_id": client_order_id,
                        },
                    )
                return

            self._daily_trade_count += 1
            logger.info(
                f"LiveEngine: order executed — {_signal.direction.name}, "
                f"qty={quantity}, order_id={order.order_id}"
            )

            # Log fill to journal (P2.1)
            slippage = None
            if order.price is not None:
                slippage = order.price - _signal.entry_price
                if side == OrderSide.SELL:
                    slippage = -slippage

            if self._audit_trail is not None:
                self._audit_trail.log(
                    _event_type="FILL",
                    _cycle_number=self._cycle_count,
                    _direction=_signal.direction.name,
                    _entry_price=_signal.entry_price,
                    _stop_loss=_signal.stop_loss,
                    _take_profit=_signal.take_profit,
                    _quantity=quantity,
                    _order_id=order.order_id,
                    _fill_price=order.price,
                    _slippage=slippage,
                    _equity=equity,
                    _details={
                        "filled_quantity": order.filled_quantity,
                        "average_fill_price": order.average_fill_price,
                    },
                )

            # --- Track slippage (P1.2) ---
            if self._slippage_tracker is not None and order.price is not None:
                self._slippage_tracker.record(
                    _symbol=self._broker_symbol,
                    _side=side,
                    _expected_price=_signal.entry_price,
                    _actual_price=order.price,
                    _quantity=quantity,
                    _strategy=self._strategy_name,
                )

        except Exception as e:
            logger.error(f"LiveEngine: order execution failed — {e}")
            print(f"  OrderExecutor: ERROR order failed: {e}")

            # Log error to journal (P2.1)
            if self._audit_trail is not None:
                self._audit_trail.log(
                    _event_type="ENGINE",
                    _cycle_number=self._cycle_count,
                    _reason=f"ORDER_FAILED: {str(e)}",
                    _direction=_signal.direction.name,
                    _entry_price=_signal.entry_price,
                    _quantity=quantity,
                )

    def _check_exit_conditions(self) -> None:
        """
        Check dynamic exit conditions for open positions.

        Beyond SL/TP (which the broker handles intrabar), the strategy
        may have additional exit logic (e.g., EMA cross reversal,
        regime change). These are evaluated at bar close.
        """
        positions = self._state_reconciler.get_positions(
            _symbol=self._broker_symbol, _strategy=self._strategy_name,
        )

        if not positions:
            return

        for pos in positions:
            direction = (TradeDirection.LONG if pos.side.name == 'LONG'
                         else TradeDirection.SHORT)

            # Check if strategy has check_exit_condition
            if hasattr(self._strategy, 'check_exit_condition'):
                should_exit, reason = self._strategy.check_exit_condition(
                    _bar_index=len(self._data) - 1,
                    _position_side=direction,
                )
                if should_exit:
                    logger.info(
                        f"LiveEngine: closing position {pos.position_id} — "
                        f"reason: {reason}"
                    )
                    print(f"  Exit: closing {pos.position_id} — {reason}")

                    # Log exit to journal (P2.1)
                    if self._audit_trail is not None:
                        self._audit_trail.log(
                            _event_type="EXIT",
                            _cycle_number=self._cycle_count,
                            _direction=direction.name,
                            _order_id=pos.position_id,
                            _reason=reason,
                        )

                    try:
                        self._broker.close_position(_position_id=pos.position_id)
                    except Exception as e:
                        logger.error(
                            f"LiveEngine: failed to close position — {e}"
                        )
                        print(f"  Exit: ERROR close failed: {e}")

    def shutdown(self) -> None:
        """
        Clean shutdown of the live trading engine.

        Does NOT close open positions — that's a manual or kill-switch decision.
        Stops heartbeat monitor if running.
        Stops kill switch monitor if running (P2.2).
        Prints slippage summary if tracking was enabled.
        Prints execution quality summary if monitoring was enabled (P2.2).
        Saves execution quality to parquet (P2.2).
        Flushes trade journal to parquet (P2.1).
        Prints paper trading summary if paper mode was enabled (P3.1).
        """
        self._running = False

        # Stop heartbeat monitor
        if self._heartbeat is not None:
            self._heartbeat.stop()

        # P2.2: Stop kill switch monitor
        if self._kill_switch is not None:
            self._kill_switch.stop_monitor()

        # Print slippage summary
        if self._slippage_tracker is not None and self._slippage_tracker.record_count > 0:
            summary = self._slippage_tracker.get_summary()
            print(f"\n--- Slippage Summary ---")
            print(f"  Total fills tracked: {summary['total_records']}")
            print(f"  Mean slippage: {summary['mean_slippage']:.5f} "
                  f"({summary['mean_slippage_pct']:.4%})")
            print(f"  Max slippage (worst): {summary['max_slippage']:.5f}")
            print(f"  Min slippage (best): {summary['min_slippage']:.5f}")
            print(f"  Total slippage cost: {summary['total_slippage_cost']:.2f}")
            logger.info(f"LiveEngine: slippage summary — {summary}")

        # --- P2.2: Execution quality summary ---
        if self._execution_monitor is not None and self._execution_monitor.record_count > 0:
            eq_report = self._execution_monitor.get_report()
            print(f"\n--- Execution Quality Summary ---")
            print(f"  Broker: {eq_report['broker_name']}")
            print(f"  Submissions: {eq_report['total_submissions']}")
            print(f"  Fills: {eq_report['total_fills']}")
            print(f"  Rejections: {eq_report['total_rejections']} "
                  f"({eq_report['rejection_rate']:.2%})")
            print(f"  Requotes: {eq_report['total_requotes']} "
                  f"({eq_report['requote_rate']:.2%})")
            print(f"  Avg fill time: {eq_report['avg_fill_time_ms']:.1f}ms "
                  f"(median={eq_report['median_fill_time_ms']:.1f}ms, "
                  f"max={eq_report['max_fill_time_ms']:.1f}ms)")
            print(f"  Avg slippage: {eq_report['avg_slippage']:.5f} "
                  f"(abs={eq_report['avg_abs_slippage']:.5f})")
            if eq_report['slippage_by_hour']:
                print(f"  Slippage by hour (UTC):")
                for hour, stats in sorted(eq_report['slippage_by_hour'].items()):
                    print(f"    {hour:02d}:00 — mean={stats['mean']:.5f}, "
                          f"n={stats['count']}, std={stats['std']:.5f}")
            if eq_report['slippage_by_regime']:
                print(f"  Slippage by regime:")
                for regime, stats in eq_report['slippage_by_regime'].items():
                    print(f"    {regime} — mean={stats['mean']:.5f}, "
                          f"n={stats['count']}, std={stats['std']:.5f}")
            logger.info(f"LiveEngine: execution quality report — {eq_report}")

            # Save execution quality to parquet
            self._execution_monitor.save_to_parquet()

        # --- P2.1: Shutdown trade journal (flush to parquet) ---
        if self._audit_trail is not None:
            self._audit_trail.shutdown()

        # --- P3.1: Paper trading summary ---
        if self._paper_broker is not None:
            summary = self._paper_broker.get_paper_summary()
            print(f"\n--- Paper Trading Summary ---")
            print(f"  Initial balance: {summary['initial_balance']:.2f}")
            print(f"  Final balance:   {summary['final_balance']:.2f}")
            print(f"  Final equity:    {summary['final_equity']:.2f}")
            print(f"  Total P&L:       {summary['total_pnl']:.2f} "
                  f"({summary['total_pnl_pct']:.2%})")
            print(f"  Realized P&L:    {summary['realized_pnl']:.2f}")
            print(f"  Unrealized P&L:  {summary['unrealized_pnl']:.2f}")
            print(f"  Total trades:    {summary['total_trades']}")
            print(f"  Open positions:  {summary['open_positions']}")
            if summary['total_trades'] > 0:
                print(f"  Win rate:        {summary['win_rate']:.1%} "
                      f"({summary['winning_trades']}W / "
                      f"{summary['losing_trades']}L)")
            print(f"  Total commission: {summary['total_commission']:.2f}")
            logger.info(f"LiveEngine: paper trading summary — {summary}")

        logger.info(
            f"LiveEngine: shutdown. "
            f"Completed {self._cycle_count} cycles, "
            f"{self._daily_trade_count} trades today."
        )
        print(f"\n{'='*60}")
        print(f"LIVE ENGINE SHUTDOWN")
        print(f"Cycles completed: {self._cycle_count}")
        print(f"Trades today: {self._daily_trade_count}")
        print(f"{'='*60}")

    @property
    def is_running(self) -> bool:
        """Check if engine is currently running."""
        return self._running

    @property
    def cycle_count(self) -> int:
        """Get the number of completed cycles."""
        return self._cycle_count

    @property
    def slippage_tracker(self) -> Optional[SlippageTracker]:
        """Access the slippage tracker (for external analysis)."""
        return self._slippage_tracker

    @property
    def heartbeat(self) -> Optional[HeartbeatMonitor]:
        """Access the heartbeat monitor (for external status checks)."""
        return self._heartbeat

    @property
    def audit_trail(self) -> Optional[AuditTrail]:
        """Access the trade journal (for external queries) (P2.1)."""
        return self._audit_trail

    @property
    def spread_filter(self) -> Optional[SpreadFilter]:
        """Access the spread filter (P2.1)."""
        return self._spread_filter

    @property
    def kill_switch(self) -> Optional[KillSwitch]:
        """Access the kill switch (P2.2)."""
        return self._kill_switch

    @property
    def execution_monitor(self) -> Optional[ExecutionQualityMonitor]:
        """Access the execution quality monitor (P2.2)."""
        return self._execution_monitor

    @property
    def paper_trade(self) -> bool:
        """Check if engine is in paper trading mode (P3.1)."""
        return self._paper_trade

    @property
    def paper_broker(self) -> Optional[PaperBroker]:
        """Access the paper broker (P3.1). None if live mode."""
        return self._paper_broker
