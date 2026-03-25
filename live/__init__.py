"""
Live Trading Module
====================
Live trading engine with stateless bar-close architecture.

Module Structure (P1.1 + P1.2 + P2.1 + P2.2 + P3.1 + P3.2):
    live_engine.py          - LiveTradingEngine — main orchestrator
    live_configurator.py    - LiveConfigurator — YAML-driven settings
    live_params.yaml        - Live trading parameters
    bar_timer.py            - BarTimer — bar completion detection
    data_validator.py       - DataValidator — historical data checks
    position_sizer.py       - PositionSizer — lot rounding, min/max
    order_executor.py       - OrderExecutor — order placement, SL/TP, partial fills (P2.1),
                              idempotent submission (P2.2), execution monitor integration (P2.2)
    state_reconciler.py     - StateReconciler — broker state sync
    risk_checks.py          - PreTradeRiskCheck — circuit breakers (P1.2)
    market_hours.py         - MarketHoursFilter — trading session filter (P1.2)
    slippage_tracker.py     - SlippageTracker — actual vs expected tracking (P1.2)
    heartbeat.py            - HeartbeatMonitor — connectivity monitoring (P1.2)
    audit_trail.py          - AuditTrail — audit trail, CSV + parquet (P2.1)
    spread_filter.py        - SpreadFilter — bid-ask spread check (P2.1)
    kill_switch.py          - KillSwitch — emergency flatten (P2.2)
    execution_monitor.py    - ExecutionQualityMonitor — fill latency, rejection
                              rate, requote frequency, slippage by hour/regime (P2.2)
    paper_broker.py         - PaperBroker — simulated fill engine for paper
                              trading mode (P3.1)
    shadow_runner.py        - ShadowRunner — backtest-live parity validation (P3.2)
    shadow_report.py        - ShadowParityReport — parity comparison report (P3.2)

Classes:
    LiveTradingEngine       - Main orchestrator for live trading
    LiveConfigurator        - YAML-driven live parameter loader
    BarTimer                - Detects bar completion at timeframe boundaries
    DataValidator           - Validates historical data recency and warmup
    PositionSizer           - Calculates position size with lot rounding
    OrderExecutor           - Handles order submission with SL/TP + partial fills
                              + idempotent submission + execution quality tracking
    StateReconciler         - Reconciles local state with broker
    PreTradeRiskCheck       - Gate between signal and order execution
    MarketHoursFilter       - Prevents trading outside market hours
    SlippageTracker         - Tracks actual vs expected fill prices
    HeartbeatMonitor        - Proactive broker connectivity monitoring
    AuditTrail              - Append-only audit trail (CSV + daily parquet)
    SpreadFilter            - Pre-trade bid-ask spread check
    KillSwitch              - Emergency flatten (close all, cancel all, halt)
    ExecutionQualityMonitor - Per-broker execution quality metrics
    PaperBroker             - Simulated fill engine for paper trading (P3.1)
    ShadowRunner            - Backtest-live parity validator (P3.2)
    ShadowParityReport      - Parity comparison report (P3.2)

Usage:
    from live import LiveTradingEngine, LiveConfigurator
    config = LiveConfigurator(_path="live/live_params.yaml")
    engine = LiveTradingEngine(
        _broker=broker, _strategy=strategy, _data_manager=data_manager,
        _config=config, _data_configurator=data_cfg, _brokers_configurator=brokers_cfg,
        _risk_pct=0.005, _max_trades_per_day=0, _contract_size=100.0,
    )
    engine.run()

    # P3.2: Shadow mode (backtest-live parity validation)
    from live import ShadowRunner
    runner = ShadowRunner(
        _strategy_class=MyStrategy, _strategy_params={...},
        _data=df, _instrument=instrument_meta,
    )
    report = runner.run()
    report.print_summary()
"""

from .live_configurator import LiveConfigurator
from .bar_timer import BarTimer
from .data_validator import DataValidator
from .position_sizer import PositionSizer
from .order_executor import OrderExecutor
from .state_reconciler import StateReconciler
from .risk_checks import PreTradeRiskCheck
from .market_hours import MarketHoursFilter, MarketStatus
from .slippage_tracker import SlippageTracker, SlippageRecord
from .heartbeat import HeartbeatMonitor
from .audit_trail import AuditTrail
from .spread_filter import SpreadFilter
from .kill_switch import KillSwitch
from .execution_monitor import ExecutionQualityMonitor, ExecutionRecord
from .paper_broker import PaperBroker
from .shadow_runner import ShadowRunner
from .shadow_report import ShadowParityReport, SignalRecord, PositionSizeRecord, ParityMismatch
from .live_engine import LiveTradingEngine

__all__ = [
    'LiveTradingEngine',
    'LiveConfigurator',
    'BarTimer',
    'DataValidator',
    'PositionSizer',
    'OrderExecutor',
    'StateReconciler',
    'PreTradeRiskCheck',
    'MarketHoursFilter',
    'MarketStatus',
    'SlippageTracker',
    'SlippageRecord',
    'HeartbeatMonitor',
    'AuditTrail',
    'SpreadFilter',
    'KillSwitch',
    'ExecutionQualityMonitor',
    'ExecutionRecord',
    'PaperBroker',
    'ShadowRunner',
    'ShadowParityReport',
    'SignalRecord',
    'PositionSizeRecord',
    'ParityMismatch',
]
