# Systematic Trading Infrastructure

**End-to-end infrastructure for systematic trading — from data ingestion to live execution — built to the operational standards of a small/mid-sized fund or proprietary trading desk.**

Multi-broker, multi-asset, event-driven. Designed so that the same strategy object runs unchanged in backtest, paper and live, and so that every live decision is reconstructable after the fact.

---

## Contents

- [What this is](#what-this-is)
- [Design principles](#design-principles)
- [Architecture](#architecture)
- [Module status](#module-status)
- [Key design decisions](#key-design-decisions)
- [Production concerns](#production-concerns)
- [Configuration architecture](#configuration-architecture)
- [Testing and reproducibility](#testing-and-reproducibility)
- [Roadmap](#roadmap)
- [Technical documentation](#technical-documentation-available-on-request)
- [Code availability](#code-availability)

---

## What this is

A complete systematic trading stack built from scratch in Python, covering the full lifecycle:

| Layer | Capability |
|---|---|
| **Data management** | Parquet/DuckDB storage, incremental sync, gap detection, automated quality checks |
| **Broker abstraction** | One interface across Interactive Brokers, Oanda, Yahoo Finance and two additional venues |
| **Strategy framework** | Stateful strategies with a strict, testable lifecycle contract |
| **Backtesting** | Custom event-driven engine with intrabar fills and an explicit cost model |
| **Optimization** | Walk-forward optimization with out-of-sample validation |
| **Portfolio construction** | Selection and allocation across uncorrelated strategies |
| **Live trading** | Real-time loop with pre-trade risk, state reconciliation, crash recovery and full audit trail |
| **Reporting** | Metrics, equity/drawdown charts, interactive trade viewer, CSV export |

### Scope

**In scope.** Bar-based systematic strategies on liquid instruments across FX, equities, futures and CFDs. Timeframes from intraday minutes to daily. Single-node deployment operating a portfolio of strategies with automated execution and supervision.

**Deliberately out of scope.** Sub-millisecond / co-located execution, order-book microstructure and market making, options greeks and volatility surface modelling, tick-level data storage, multi-tenant or multi-user operation. Each of these would change the architecture materially and none is required by the target strategy class.

### Target scale

Sized for a single portfolio manager or a small quantitative team: tens of instruments, tens of concurrent strategy instances, minute-to-daily bars. The engineering practices — interface contracts, state reconciliation, audit trail, shadow running — are those of an institutional desk; the operational footprint is deliberately not.

---

## Design principles

1. **The same object runs everywhere.** A strategy is written once and executed unchanged by the backtest engine, the paper broker and the live engine. Divergence between research and production is an architectural failure, not a tuning problem.
2. **Broker truth wins.** Internal state is a cache. On every reconciliation cycle the broker's positions and orders are authoritative, and any divergence is an event that must be surfaced and resolved before trading continues.
3. **Configuration is data, not code.** Every module owns a YAML file and a configurator that validates it. No parameter is ever hard-coded in a strategy or an engine.
4. **Fail closed.** Stale data, a widened spread, a failed heartbeat or an unreconciled position stops trading. The system's default response to uncertainty is to do nothing.
5. **Interfaces before implementations.** Every pluggable concern — data source, broker, indicator, strategy, exit rule — sits behind an abstract base class with an explicit contract. Adding a venue is an implementation exercise, never a refactor.
6. **Every live decision is reconstructable.** Bar, signal, risk verdict, order, fill and resulting state are written to an append-only audit trail. Post-mortems are a query, not an investigation.
7. **Optimism is a cost, not a parameter.** Fills, costs, timing and data availability are modelled pessimistically by default. The backtest is a floor, not a forecast.

---

## Architecture

### Component overview

```
                         ┌───────────────────────────────┐
                         │        Configuration          │
                         │  YAML + per-module validators │
                         └───────────────┬───────────────┘
                                         │  (injected everywhere)
   ┌─────────────────────────────────────┴───────────────────────────────────┐
   │                                                                         │
┌──┴───────────────┐   ┌──────────────────┐   ┌──────────────┐   ┌───────────┴──────┐
│      DATA        │   │   INDICATORS     │   │  STRATEGIES  │   │     BROKERS      │
│  DataManager     │──>│  IndicatorBase   │──>│ StrategyBase │<─>│   BrokerBase     │
│  DataSourceBase  │   │  trend/momentum  │   │   Signal     │   │  IB · Oanda ·    │
│  ParquetHandler  │   │  volatility      │   │ MultiExit    │   │  Paper           │
│  DuckDBHandler   │   └──────────────────┘   └──────┬───────┘   └───────┬──────────┘
│  QualityChecker  │                                 │                   │
└──────────────────┘                                 │                   │
                                    ┌────────────────┴─────────┬─────────┴──────────┐
                                    │                          │                    │
                            ┌───────┴────────┐        ┌────────┴────────┐  ┌────────┴────────┐
                            │   BACKTEST     │        │  OPTIMIZATION   │  │      LIVE       │
                            │ Event-driven   │───────>│  Walk-forward   │  │  LiveEngine     │
                            │ Intrabar fills │        │  Grid search    │  │  Risk · Recon · │
                            │ Cost model     │        │  IS / OOS       │  │  Audit · Kill   │
                            └───────┬────────┘        └────────┬────────┘  └────────┬────────┘
                                    │                          │                    │
                                    └──────────┬───────────────┴────────────────────┘
                                               │
                              ┌────────────────┴─────────────────────────┐
                              │   PORTFOLIO  ·  REPORTING  ·  CHARTING   │
                              └──────────────────────────────────────────┘
```

### Repository structure

```
trading_system/
├── README.md
├── requirements.txt
├── __main__.py                        # Main entry point with usage examples
├── core/
│   ├── data_types.py                  # Timeframe, OrderType, InstrumentMetadata, etc.
│   └── exceptions.py                  # ConfigurationError, DataError, BrokerError, etc.
├── data/
│   ├── data_configurator.py           # DataConfigurator - instrument metadata
│   ├── data_manager.py                # DataManager - download/sync/query data
│   ├── data_source_base.py            # DataSourceBase - abstract data source
│   ├── data_ib.py                     # DataSourceIB
│   ├── data_oanda.py                  # DataSourceOanda
│   ├── data_yahoo.py                  # DataSourceYahoo
│   ├── parquet_handler.py             # ParquetHandler
│   ├── duckdb_handler.py              # DuckDBHandler
│   ├── data_quality.py                # DataQualityChecker, QualityReport
│   ├── timeframe_ops.py               # TimeframeManager
│   ├── instruments.yaml               # Instrument definitions per broker
│   └── ohlcv/                         # Data storage (source/symbol.parquet)
├── brokers/
│   ├── brokers_configurator.py        # BrokersConfigurator - broker config
│   ├── broker_manager.py              # BrokerManager - create broker instances
│   ├── broker_base.py                 # BrokerBase - abstract broker
│   ├── broker_ib.py                   # BrokerIB
│   ├── broker_oanda.py                # BrokerOanda
│   └── brokers.yaml                   # Broker connection settings
├── indicators/
│   ├── indicator_base.py              # IndicatorBase - abstract indicator
│   ├── trend_indicators.py
│   ├── momentum_indicators.py
│   └── volatility_indicators.py
├── strategies/
│   ├── strategy_configurator.py       # StrategyConfigurator
│   ├── strategy_base.py               # StrategyBase, Signal, TradeDirection
│   ├── multi_exit.py                  # MultiExitManager, ExitRule, ExitType
│   ├── strategy_params.yaml           # Strategy parameters (R&D)
│   └── strategy_params_prod.yaml      # Strategy parameters (Production)
├── backtest/
│   ├── backtest_configurator.py       # BacktestConfigurator
│   ├── backtest_engine.py             # BacktestEngine, BacktestResult, Trade
│   ├── display_backtest.py            # display_backtest_results functions
│   └── backtest_params.yaml           # Backtest settings
├── optimization/
│   ├── optimization_configurator.py   # OptimizationConfigurator
│   ├── walk_forward.py                # WalkForwardOptimizer, WalkForwardResult
│   ├── grid_search.py                 # ParameterGrid, GridSearchOptimizer
│   ├── display_walkforward.py         # display_walkforward_results functions
│   ├── display_advanced.py            # display_advanced_wf_results, charts
│   └── optimization_params.yaml       # WF settings, filtering, param grids
├── portfolio/
│   ├── portfolio_configurator.py      # PortfolioConfigurator
│   └── portfolio_params.yaml          # Portfolio construction settings
├── charting/
│   ├── __init__.py                    # launch_chart, ChartServer, prepare_chart_data
│   └── chart_server.py                # Dash + Plotly: price and trades viewer
├── live/
│   ├── live_engine.py                 # LiveTradingEngine — main orchestrator
│   ├── live_configurator.py           # LiveConfigurator — YAML-driven settings
│   ├── live_params.yaml               # Live trading parameters
│   ├── bar_timer.py                   # BarTimer — bar completion detection
│   ├── data_validator.py              # DataValidator — data recency/warmup
│   ├── position_sizer.py              # PositionSizer — lot rounding, min/max
│   ├── order_executor.py              # OrderExecutor — orders, SL/TP, partial fills
│   ├── state_reconciler.py            # StateReconciler — broker state sync
│   ├── risk_checks.py                 # PreTradeRiskCheck — circuit breakers
│   ├── market_hours.py                # MarketHoursFilter — session filter
│   ├── slippage_tracker.py            # SlippageTracker — fill tracking
│   ├── heartbeat.py                   # HeartbeatMonitor — connectivity
│   ├── audit_trail.py                 # AuditTrail — audit trail CSV+parquet
│   ├── spread_filter.py               # SpreadFilter — bid-ask spread check
│   ├── kill_switch.py                 # KillSwitch — emergency flatten
│   ├── execution_monitor.py           # ExecutionQualityMonitor — fill metrics
│   ├── paper_broker.py                # PaperBroker — simulated fill engine
│   ├── shadow_runner.py               # ShadowRunner — backtest-live parity
│   └── shadow_report.py               # ShadowParityReport — parity report
├── reporting/
│   ├── metrics_calculator.py          # MetricsCalculator, PerformanceMetrics
│   ├── csv_exporter.py                # CSVExporter
│   └── report_generator.py            # ReportGenerator
├── utils/
│   ├── logging.py                     # TradeLogger
│   └── validators.py                  # Symbol, timeframe, price, quantity, dates
└── tests/
    ├── test_config.py
    ├── test_data_manager.py
    ├── test_brokers.py
    ├── test_indicators.py
    ├── test_strategies.py
    ├── test_backtest.py
    └── test_walk_forward.py
```

### Two paths, one core

The system has exactly two execution paths, and they share every component that could otherwise drift apart:

**Research path** — `DataManager → Indicators → Strategy → BacktestEngine → WalkForwardOptimizer → Portfolio → Reporting`

**Production path** — `DataManager → DataValidator → Indicators → Strategy → RiskChecks → PositionSizer → OrderExecutor → Broker → StateReconciler → AuditTrail`

The strategy, indicator and data layers are byte-identical between the two. The production path adds validation, risk and reconciliation stages; it never adds a second implementation of anything already used in research. The `ShadowRunner` exists to prove this continuously — see [Production concerns](#production-concerns).

### Live loop

```
  Bar close detected (BarTimer)
        │
        ├─▶ DataValidator      recency, warmup depth, gap check      ──▶ fail: skip cycle
        ├─▶ MarketHoursFilter  session open, no scheduled halt       ──▶ fail: skip cycle
        ├─▶ SpreadFilter       bid-ask within tolerance              ──▶ fail: skip cycle
        │
        ├─▶ Strategy.on_bar()  → Signal | None
        │
        ├─▶ PreTradeRiskCheck  exposure, drawdown, order rate, kill  ──▶ fail: block + alert
        ├─▶ PositionSizer      lot rounding, min/max, tick alignment
        ├─▶ OrderExecutor      submit, attach SL/TP, track partials
        │
        ├─▶ StateReconciler    broker positions vs. internal state   ──▶ divergence: halt
        ├─▶ SlippageTracker    expected vs. realised fill
        └─▶ AuditTrail         append-only record of the full cycle
```

Every stage is independently testable and independently disableable via configuration. A failure at any gate produces a logged, audited no-trade outcome rather than an exception that stops the engine.

---

## Module status

| Module | Status | Notes |
|---|---|---|
| `core` | Stable | Type system, enums, exception hierarchy |
| `data` | Stable | Multi-source ingestion, quality checks, timeframe operations |
| `brokers` | Stable | IB and Oanda live; two additional venues optional |
| `indicators` | Stable | Trend, momentum, volatility families |
| `strategies` | Stable | Base contract, exit management, parameter separation R&D/prod |
| `backtest` | Stable | Event-driven engine, intrabar fills, cost model |
| `optimization` | Stable | Walk-forward, grid search, result reporting |
| `live` | Stable | Full production loop, 18 components, shadow parity harness |
| `reporting` | Stable | Metrics, exports, report generation |
| `charting` | Stable | Static charts and interactive Dash/Plotly viewer |
| `portfolio` | In development | Configurator and parameter schema in place; selection logic in progress |
| Observability stack | Planned | Structured metrics and alerting beyond file logging |
| CI / packaging | Planned | Test automation and containerised deployment |

Status is stated plainly because it is more useful than a green checkmark. Nothing marked *stable* is a prototype; nothing marked *planned* is claimed as delivered.

---

## Key design decisions

The decisions below shaped the architecture. Each is recorded in full — context, alternatives, trade-off accepted — in the design decision log.

| # | Decision | Chosen | Rejected | Trade-off accepted |
|---|---|---|---|---|
| 1 | Backtest paradigm | Event-driven | Vectorized | 10–100× slower; buys path-dependent state, realistic fills and research/production parity |
| 2 | Backtest engine | Custom | Backtrader, vectorbt, Zipline | Build and maintenance cost; buys full control of the fill model and no hidden lookahead |
| 3 | Storage | Parquet + DuckDB | TimescaleDB, InfluxDB, CSV | No concurrent writers; buys zero-ops deployment and columnar analytical speed |
| 4 | Broker integration | Abstract base + per-venue adapters | Direct SDK calls | Adapter layer to maintain; buys venue independence and a testable paper broker |
| 5 | Configuration | Per-module YAML + configurator | Single global config, code constants | More files; buys module isolation, validation at load, and prod/R&D separation |
| 6 | Paper broker | First-class `BrokerBase` implementation | Mocking in tests only | Fill simulation must be maintained; buys a real dress-rehearsal environment |
| 7 | Live state model | Broker as source of truth, reconciled each cycle | Internal state authoritative | Extra API calls per cycle; buys correct recovery from crashes and manual intervention |
| 8 | Optimization protocol | Walk-forward with strict out-of-sample | Single-split train/test, full-sample fitting | Far fewer usable observations; buys an honest estimate of degradation |
| 9 | Concurrency | Single-threaded live loop, parallel research | Async live engine | Throughput ceiling; buys deterministic ordering and trivially debuggable production behaviour |
| 10 | Data adjustment | Store raw, adjust on read | Store adjusted | Read-time cost; buys point-in-time correctness and re-derivable history |

---

## Production concerns

This is the section that separates a backtesting toolkit from trading infrastructure.

**Correctness under failure**
- **State reconciliation** — broker positions and working orders are re-read and diffed against internal state every cycle; divergence halts trading rather than trading through it.
- **Crash recovery** — engine state is persisted continuously and rebuilt from broker truth on restart, so a mid-session process death is recoverable without manual position archaeology.
- **Idempotent order submission** — client order IDs are deterministic, so a retry after an ambiguous network failure cannot duplicate an order.
- **Partial fill handling** — positions are tracked at fill granularity, not order granularity, with protective orders resized accordingly.

**Risk controls**
- **Pre-trade checks** — exposure limits, concentration, order rate, drawdown thresholds and daily loss limits, evaluated before every submission.
- **Circuit breakers** — breaching a configured threshold suspends new entries while leaving exit logic active.
- **Kill switch** — a single externally-triggerable control that flattens all positions, cancels all working orders and refuses further entries.
- **Session and spread filters** — no execution outside configured hours or when the spread exceeds instrument-specific tolerance.

**Execution quality**
- **Slippage tracking** — expected versus realised fill recorded per trade, aggregated per instrument, venue and session.
- **Execution monitor** — fill rates, rejection reasons and latency distribution surfaced as ongoing metrics rather than end-of-month surprises.

**Backtest–live parity**
- **Shadow runner** — the live engine runs against live data with execution suppressed, in parallel with a backtest over the same window.
- **Parity report** — signal-level and timing-level divergence between the two is quantified and thresholded. A strategy is not promoted to capital until parity is demonstrated. This is the mechanism that turns principle 1 from an intention into a tested property.

**Auditability**
- **Append-only audit trail** — every cycle writes bar context, signal, risk verdict, sizing inputs, order lifecycle and resulting state to CSV and Parquet.
- **Reconstructable decisions** — any historical trade can be replayed from the trail to the inputs that produced it, which is the practical requirement behind most recordkeeping obligations.

**Monitoring**
- **Heartbeat** — connectivity, data flow and loop liveness monitored continuously, with degradation treated as a trading-halt condition.

---

## Configuration architecture

Each module owns exactly one YAML file and one configurator class that loads, validates and exposes it. Configurators fail loudly at startup, never silently at runtime.

| Module | Configurator | YAML |
|---|---|---|
| `data` | `DataConfigurator` | `data/instruments.yaml` |
| `brokers` | `BrokersConfigurator` | `brokers/brokers.yaml` |
| `strategies` | `StrategyConfigurator` | `strategies/strategy_params.yaml` · `strategy_params_prod.yaml` |
| `backtest` | `BacktestConfigurator` | `backtest/backtest_params.yaml` |
| `optimization` | `OptimizationConfigurator` | `optimization/optimization_params.yaml` |
| `portfolio` | `PortfolioConfigurator` | `portfolio/portfolio_params.yaml` |
| `live` | `LiveConfigurator` | `live/live_params.yaml` |

Research and production parameters are held in separate files by design. Promoting a parameter set from research to production is an explicit, reviewable, diffable act — not an edit to a shared file.

---

## Testing and reproducibility

- **Unit tests** across configuration, data management, brokers, indicators, strategies, backtest and walk-forward.
- **Paper broker as a test double with teeth** — integration tests run the full live loop against a simulated fill engine, exercising order lifecycle, partial fills and reconciliation paths that mocks cannot reach.
- **Deterministic backtests** — identical inputs and configuration produce identical results; seeds and configuration snapshots are recorded with each run.
- **Data quality gates** — ingestion runs produce a structured quality report; a failed check blocks downstream use rather than propagating quietly.
- *(Planned)* CI pipeline, coverage reporting, enforced typing and pre-commit hooks.
- *(Planned)* Property-based tests for sizing, rounding and reconciliation invariants.

---

## Roadmap

Ordered by expected value, not by ease.

**Now** — Portfolio construction: correlation-based selection across strategies, risk-budgeted allocation, portfolio-level exposure constraints.

**Next** — Observability: structured event stream, metric emission and alert routing, replacing file-log inspection as the primary operational interface.

**Then** *(planned, second-order priority)*
- CI pipeline with automated test execution and coverage gates
- Containerised deployment and scheduled operation
- Externalised secrets management
- Corporate-actions and adjustment handling for the equities path
- Formal run manifests binding results to code version, configuration and data snapshot
- Distributed optimization across multiple workers

These are deliberate sequencing choices. The infrastructure was built from scratch and prioritised toward correctness of the trading loop first; operational tooling around it is the next tier of work rather than an oversight.

---

## Technical documentation (available on request)

Detailed design documentation exists for each area below and is available on request.

| Document | Contents |
|---|---|
| Architecture | Layering, dependency rules, interface contracts, extension points, type system |
| Design decisions | ADR-style record of the ten decisions above, with alternatives and costs accepted |
| Data engineering | Storage layout, sync, quality checks, timezone and point-in-time handling |
| Broker and execution | Broker contract, capability matrix, order mapping, connection lifecycle |
| Backtest and walk-forward | Fill model, cost model, lookahead safeguards, walk-forward protocol |
| Live operations | Live loop, risk controls, failure modes, runbook, go-live checklist |
| Testing and reproducibility | Test strategy, determinism, quality gates |
| Roadmap | Prioritisation reasoning and deferred work |

---

## Code availability

The publicly shared code covers `core`, `data`, `brokers` and `reporting` — the standard infrastructure layers, sufficient to evaluate design quality, interface discipline and engineering standards.

`strategies`, `optimization`, `portfolio` and `live` are kept private, as they encapsulate methodological edge. Though, the documentation above describes their architecture and operational properties in full; the implementations are available for discussion under appropriate agreements.

---

## Stack

Python · pandas · NumPy · PyArrow/Parquet · DuckDB · Dash · Plotly · PyYAML · pytest · `ib_insync` · Oanda v20 API
