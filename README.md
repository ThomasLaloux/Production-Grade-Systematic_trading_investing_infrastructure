# Productio-Grade Systematic Trading Infrastructure - Architecture Design

## Overview
Core infrastructure for a systematic trading/investing system (on-going development)
- **Data Management**: Parquet/DuckDB storage with data quality checks
- **Broker Abstraction**: Unified interface for Interactive Brokers, Yahoo Finance, Oanda and two optional brokers
- **Strategy Framework**: Stateful ready strategies
- **Backtesting Engine**: Event-driven backtesting with intrabar fills, custom backtesting engine
- **Walk-Forward Optimization**: Walk-forward optimization
- **Portfolio Construction**: (Un)correlation-based portfolio building
- **Live Trading Engine**: Real-time data collection, signal assessment, order execution, position management, crash recovery, audit trail
- **Charting**: Static (equity curve, max drawdown, performance metrics), dynamic (signals visualization)
- **Reporting**: Metrics calculation, chart generation (equity/balance curves, drawdown), CSV export

## Project Structure
```
trading_system/
├── README.md
├── requirements.txt
├── __main__.py                        # Main entry point with usage examples
├── core/
│   ├── __init__.py
│   ├── data_types.py                  # Timeframe, OrderType, InstrumentMetadata, etc.
│   └── exceptions.py                  # ConfigurationError, DataError, BrokerError, etc.
├── data/
│   ├── __init__.py
│   ├── data_configurator.py           # DataConfigurator - instrument metadata
│   ├── data_manager.py                # DataManager - download/sync/query data
│   ├── data_source_base.py            # DataSourceBase - abstract data source
│   ├── data_ib.py                     # DataSourceIB
│   ├── data_yahoo.py                  # DataSourceYahoo
│   ├── data_oanda.py                  # DataSourceOanda
│   ├── parquet_handler.py             # ParquetHandler
│   ├── duckdb_handler.py              # DuckDBHandler
│   ├── data_quality.py                # DataQualityChecker, QualityReport
│   ├── timeframe_ops.py               # TimeframeManager
│   ├── instruments.yaml               # Instrument definitions per broker
│   └── ohlcv/                         # Data storage (source/symbol.parquet)
├── brokers/
│   ├── __init__.py
│   ├── brokers_configurator.py        # BrokersConfigurator - broker config
│   ├── broker_manager.py              # BrokerManager - create broker instances
│   ├── broker_base.py                 # BrokerBase - abstract broker
│   ├── broker_ib.py                   # BrokerIB
│   ├── broker_oanda.py                # BrokerOanda
│   └── brokers.yaml                   # Broker connection settings
├── indicators/
│   ├── __init__.py
│   ├── indicator_base.py              # IndicatorBase - abstract indicator
│   ├── trend_indicators.py            # SMA (testing purpose)
│   ├── momentum_indicators.py
│   └── volatility_indicators.py
├── strategies/
│   ├── __init__.py
│   ├── strategy_configurator.py       # StrategyConfigurator
│   ├── strategy_base.py               # StrategyBase, Signal, TradeDirection
│   ├── sma_cross.py                   # SMACrossStrategy (testing purpose)
│   ├── multi_exit.py                  # MultiExitManager, ExitRule, ExitType
│   ├── strategy_params.yaml           # Strategy parameters (R&D)
│   └── strategy_params_prod.yaml      # Strategy parameters (Production)
├── backtest/
│   ├── __init__.py
│   ├── backtest_configurator.py       # BacktestConfigurator
│   ├── backtest_engine.py             # BacktestEngine, BacktestResult, Trade
│   ├── display_backtest.py            # display_backtest_results functions
│   └── backtest_params.yaml           # Backtest settings
├── optimization/
│   ├── __init__.py
│   ├── optimization_configurator.py   # OptimizationConfigurator
│   ├── walk_forward.py                # WalkForwardOptimizer, WalkForwardResult
│   ├── grid_search.py                 # ParameterGrid, GridSearchOptimizer
│   ├── display_walkforward.py         # display_walkforward_results functions
│   ├── display_advanced.py            # display_advanced_wf_results, charts (Phase 3)
│   └── optimization_params.yaml       # WF settings, filtering, param grids
├── portfolio/
│   ├── __init__.py
│   ├── portfolio_configurator.py      # PortfolioConfigurator
│   └── portfolio_params.yaml          # Portfolio construction settings
├── charting/
│   ├── __init__.py                    # Module entry: launch_chart, ChartServer, prepare_chart_data
│   └── chart_server.py                # Dash + Plotly: price and trades viewer
├── live/
│   ├── __init__.py
│   ├── live_engine.py                 # LiveTradingEngine — main orchestrator
│   ├── live_configurator.py           # LiveConfigurator — YAML-driven settings
│   ├── live_params.yaml               # Live trading parameters
│   ├── bar_timer.py                   # BarTimer — bar completion detection
│   ├── data_validator.py              # DataValidator — data recency/warmup
│   ├── position_sizer.py              # PositionSizer — lot rounding, min/max
│   ├── order_executor.py              # OrderExecutor — order placement, SL/TP, partial fills
│   ├── state_reconciler.py            # StateReconciler — broker state sync
│   ├── risk_checks.py                 # PreTradeRiskCheck — circuit breakers
│   ├── market_hours.py                # MarketHoursFilter — session filter
│   ├── slippage_tracker.py            # SlippageTracker — fill tracking
│   ├── heartbeat.py                   # HeartbeatMonitor — connectivity
│   ├── audit_trail.py               # AuditTrail — audit trail CSV+parquet
│   ├── spread_filter.py              # SpreadFilter — bid-ask spread check
│   ├── kill_switch.py                # KillSwitch — emergency flatten
│   ├── execution_monitor.py          # ExecutionQualityMonitor — fill metrics
│   ├── paper_broker.py               # PaperBroker — simulated fill engine
│   ├── shadow_runner.py              # ShadowRunner — backtest-live parity
│   └── shadow_report.py              # ShadowParityReport — parity report
├── reporting/
│   ├── __init__.py
│   ├── metrics_calculator.py          # MetricsCalculator, PerformanceMetrics
│   ├── csv_exporter.py                # CSVExporter
│   └── report_generator.py            # ReportGenerator
├── utils/
│   ├── __init__.py
│   ├── logging.py                     # TradeLogger
│   └── validators.py                  # For symbol, timeframe, price, quantities, date range
└── tests/
    ├── __init__.py
    ├── test_config.py
    ├── test_data_manager.py
    ├── test_brokers.py
    ├── test_indicators.py
    ├── test_strategies.py
    ├── test_backtest.py
    └── test_walk_forward.py
```

## Configuration Architecture

Each module has its own configurator and YAML file:

| Module | Configurator | YAML File |
|--------|-------------|-----------|
| data | DataConfigurator | data/instruments.yaml |
| brokers | BrokersConfigurator | brokers/brokers.yaml |
| strategies | StrategyConfigurator | strategies/strategy_params.yaml |
| backtest | BacktestConfigurator | backtest/backtest_params.yaml |
| optimization | OptimizationConfigurator | optimization/optimization_params.yaml |
| portfolio | PortfolioConfigurator | portfolio/portfolio_params.yaml |
| live | LiveConfigurator | live_params.yaml |


## Two Configuration Approaches

**Approach A (R&D)**: Parameters passed directly in Python - faster iteration

**Approach B (Production)**: Parameters loaded from YAML file - auditable, version-controlled

## License
Proprietary - Internal Use Only - Open to Opportunities
