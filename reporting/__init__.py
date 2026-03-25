"""
Reporting Module
================
Performance reporting, CSV exports, and visualization for backtests.

Classes:
    PerformanceMetrics
        - to_dict
    MetricsCalculator
        - calculate_all
        - calculate_from_backtest_result
        - calculate_sharpe
        - calculate_sortino
        - calculate_calmar
        - calculate_profit_factor
        - calculate_recovery_factor
        - calculate_max_drawdown
        - calculate_ulcer_index
        - calculate_upi
        - calculate_efficiency_ratio
        - calculate_consistency_score
        - calculate_stability_factor
        - calculate_scaled_metric
        - generate_summary_report
    CSVExporter
        - export_trades
        - export_results
        - export_metrics
        - export_equity_curve
        - export_walk_forward
        - export_walk_forward_results (alias)
        - export_optimization_results
    ReportGenerator
        - generate_backtest_report
        - generate_walk_forward_report
        - plot_equity_curve
        - plot_drawdown
        - plot_monthly_returns
        - plot_trade_distribution

Usage:
    from reporting import MetricsCalculator, CSVExporter, ReportGenerator, PerformanceMetrics
    
    calc = MetricsCalculator()
    metrics = calc.calculate_from_backtest_result(backtest_result)
    
    # UPI calculation
    upi = calc.calculate_upi(_net_profit=result.net_profit, _equity_curve=pd.Series(result.equity_curve))
    ulcer = calc.calculate_ulcer_index(_equity_curve=pd.Series(result.equity_curve))
    
    # Validation metrics
    efficiency = calc.calculate_efficiency_ratio(_is_metric=2.0, _oos_metric=1.8)
    consistency = calc.calculate_consistency_score(_is_metric=2.0, _oos_metric=1.8, _alpha=0.5)
    stability = calc.calculate_stability_factor(_base_metric=2.0, _metric_values=[1.9, 2.1, 2.0])
    
    exporter = CSVExporter(_output_dir="./exports")
    exporter.export_trades(_trades=trades, _filename="trades.csv")
    exporter.export_results(_result=backtest_result, _filename="results.csv")
    exporter.export_metrics(_metrics=metrics, _filename="metrics.csv")
    exporter.export_walk_forward_results(_wf_result=wf_result, _filename="wf.csv")
    
    reporter = ReportGenerator(_output_dir="./reports")
    reporter.generate_backtest_report(_result=result, _filename="report.pdf", _title="My Strategy")
    reporter.generate_walk_forward_report(_wf_result=wf, _filename="wf_report.pdf", _title="WF Analysis")
    reporter.plot_equity_curve(_equity=equity, _filename="equity.png")
"""

from .metrics_calculator import PerformanceMetrics, MetricsCalculator
from .csv_exporter import CSVExporter
from .report_generator import ReportGenerator
from .export_manager import ExportManager

__all__ = [
    'PerformanceMetrics',
    'MetricsCalculator',
    'CSVExporter',
    'ReportGenerator',
    'ExportManager',
]
