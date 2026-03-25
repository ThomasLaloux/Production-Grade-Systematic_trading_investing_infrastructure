"""
CSV Exporter Module
===================
Export backtest results, trades, and walk-forward data to CSV format.

Classes:
    CSVExporter
        - export_trades
        - export_results
        - export_metrics
        - export_equity_curve
        - export_walk_forward
        - export_walk_forward_results (alias)
        - export_optimization_results

Usage:
    exporter = CSVExporter(_output_dir="./exports")
    exporter.export_trades(_trades=trades, _filename="trades.csv")
    exporter.export_results(_result=backtest_result, _filename="summary.csv")
    exporter.export_metrics(_metrics=performance_metrics, _filename="metrics.csv")
    exporter.export_equity_curve(_equity=equity, _timestamps=timestamps, _filename="equity.csv")
    exporter.export_walk_forward(_wf_result=wf_result, _filename="walk_forward.csv")
    exporter.export_walk_forward_results(_wf_result=wf_result, _filename="wf.csv")  # alias
"""

from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from datetime import datetime
import pandas as pd
import numpy as np

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))


class CSVExporter:
    """
    CSV exporter for backtest results and trade data.
    
    Exports trade-level details, summary results, and walk-forward analysis.
    """
    
    def __init__(self, _output_dir: str = "./exports"):
        """
        Initialize exporter.
        
        Args:
            _output_dir: Output directory for CSV files
        """
        self._output_dir = Path(_output_dir)
        self._output_dir.mkdir(parents=True, exist_ok=True)
    
    def _trade_to_dict(self, trade: Any) -> Dict[str, Any]:
        """
        Convert a trade (object or dict) to dictionary.
        
        Handles both Trade objects and dictionaries.
        
        Args:
            trade: Trade object or dictionary
            
        Returns:
            Dictionary with trade data
        """
        # If it's already a dict, return it
        if isinstance(trade, dict):
            return trade
        
        # If it has to_dict method, use it
        if hasattr(trade, 'to_dict'):
            return trade.to_dict()
        
        # Otherwise extract attributes manually
        return {
            'trade_id': getattr(trade, 'trade_id', ''),
            'symbol': getattr(trade, 'symbol', ''),
            'direction': getattr(trade, 'direction', '').name if hasattr(getattr(trade, 'direction', ''), 'name') else str(getattr(trade, 'direction', '')),
            'entry_time': getattr(trade, 'entry_time', ''),
            'entry_price': getattr(trade, 'entry_price', 0),
            'exit_time': getattr(trade, 'exit_time', ''),
            'exit_price': getattr(trade, 'exit_price', 0),
            'quantity': getattr(trade, 'quantity', 0),
            'stop_loss': getattr(trade, 'stop_loss', ''),
            'take_profit': getattr(trade, 'take_profit', ''),
            'pnl': getattr(trade, 'pnl', 0),
            'commission': getattr(trade, 'commission', 0),
            'slippage': getattr(trade, 'slippage', 0),
            'exit_reason': getattr(trade, 'exit_reason', ''),
            'mae': getattr(trade, 'mae', 0),
            'mfe': getattr(trade, 'mfe', 0),
            'bars_held': getattr(trade, 'bars_held', 0),
            'metadata': getattr(trade, 'metadata', {}),
        }
    
    def export_trades(
        self,
        _trades: List[Any],
        _filename: str = "trades.csv",
        _include_summary: bool = True,
    ) -> Path:
        """
        Export trades to CSV.
        
        Includes columns:
            - Entry/exit timestamps, price, size
            - P&L per trade, commission, slippage
            - MAE, MFE
            - Indicator values at entry/exit (from metadata)
        
        Last row contains aggregated summary if _include_summary=True.
        
        Args:
            _trades: List of Trade objects or trade dictionaries
            _filename: Output filename
            _include_summary: Add summary row at end
        
        Returns:
            Path to exported file
        """
        output_path = self._output_dir / _filename
        
        if not _trades:
            df = pd.DataFrame()
            df.to_csv(output_path, index=False)
            return output_path
        
        # Flatten metadata into columns
        rows = []
        for trade in _trades:
            # Convert to dict (handles both Trade objects and dicts)
            trade_dict = self._trade_to_dict(trade)
            
            row = {
                'trade_id': trade_dict.get('trade_id', ''),
                'symbol': trade_dict.get('symbol', ''),
                'direction': trade_dict.get('direction', ''),
                'entry_time': trade_dict.get('entry_time', ''),
                'entry_price': trade_dict.get('entry_price', 0),
                'exit_time': trade_dict.get('exit_time', ''),
                'exit_price': trade_dict.get('exit_price', 0),
                'quantity': trade_dict.get('quantity', 0),
                'stop_loss': trade_dict.get('stop_loss', ''),
                'take_profit': trade_dict.get('take_profit', ''),
                'pnl': trade_dict.get('pnl', 0),
                'commission': trade_dict.get('commission', 0),
                'slippage': trade_dict.get('slippage', 0),
                'exit_reason': trade_dict.get('exit_reason', ''),
                'mae': trade_dict.get('mae', 0),
                'mfe': trade_dict.get('mfe', 0),
                'bars_held': trade_dict.get('bars_held', 0),
            }
            
            # Add metadata columns
            metadata = trade_dict.get('metadata', {})
            if isinstance(metadata, dict):
                for key, value in metadata.items():
                    row[f'meta_{key}'] = value
            
            rows.append(row)
        
        df = pd.DataFrame(rows)
        
        # Add summary row
        if _include_summary and len(df) > 0:
            summary = self._create_trade_summary(df)
            df = pd.concat([df, pd.DataFrame([summary])], ignore_index=True)
        
        df.to_csv(output_path, index=False)
        return output_path
    
    def export_results(
        self,
        _result: Any,
        _filename: str = "results.csv",
    ) -> Path:
        """
        Export backtest results summary to CSV.
        
        Args:
            _result: BacktestResult object
            _filename: Output filename
        
        Returns:
            Path to exported file
        """
        output_path = self._output_dir / _filename
        
        # Convert result to dictionary
        if hasattr(_result, 'to_dict'):
            data = _result.to_dict()
        else:
            data = vars(_result) if hasattr(_result, '__dict__') else {}
        
        # Filter out non-scalar values
        scalar_data = {}
        for key, value in data.items():
            if isinstance(value, (int, float, str, bool)) or value is None:
                scalar_data[key] = value
            elif isinstance(value, datetime):
                scalar_data[key] = value.isoformat()
        
        df = pd.DataFrame([scalar_data])
        df.to_csv(output_path, index=False)
        return output_path
    
    def export_metrics(
        self,
        _metrics: Any,
        _filename: str = "metrics.csv",
        _strategy_name: str = "",
        _symbol: str = "",
        _timeframe: str = "",
    ) -> Path:
        """
        Export PerformanceMetrics to CSV.
        
        Args:
            _metrics: PerformanceMetrics object
            _filename: Output filename
            _strategy_name: Strategy name to include
            _symbol: Symbol to include
            _timeframe: Timeframe to include
        
        Returns:
            Path to exported file
        """
        output_path = self._output_dir / _filename
        
        # Convert metrics to dictionary
        if hasattr(_metrics, 'to_dict'):
            data = _metrics.to_dict()
        else:
            data = vars(_metrics) if hasattr(_metrics, '__dict__') else {}
        
        # Add metadata
        data['strategy_name'] = _strategy_name
        data['symbol'] = _symbol
        data['timeframe'] = _timeframe
        data['export_time'] = datetime.now().isoformat()
        
        df = pd.DataFrame([data])
        df.to_csv(output_path, index=False)
        return output_path
    
    def export_equity_curve(
        self,
        _equity: Union[pd.Series, List[float]],
        _timestamps: Optional[Union[pd.Series, List]] = None,
        _filename: str = "equity_curve.csv",
    ) -> Path:
        """
        Export equity curve to CSV.
        
        Args:
            _equity: Equity values
            _timestamps: Optional timestamps
            _filename: Output filename
        
        Returns:
            Path to exported file
        """
        output_path = self._output_dir / _filename
        
        df = pd.DataFrame({'equity': _equity})
        
        if _timestamps is not None:
            df['timestamp'] = _timestamps
            df = df[['timestamp', 'equity']]
        
        # Calculate drawdown
        rolling_max = df['equity'].expanding().max()
        df['drawdown'] = rolling_max - df['equity']
        df['drawdown_pct'] = (df['drawdown'] / rolling_max) * 100
        
        df.to_csv(output_path, index=False)
        return output_path
    
    def export_walk_forward(
        self,
        _wf_result: Any,
        _filename: str = "walk_forward.csv",
    ) -> Path:
        """
        Export walk-forward optimization results.
        
        Args:
            _wf_result: WalkForwardResult object
            _filename: Output filename
        
        Returns:
            Path to exported file
        """
        output_path = self._output_dir / _filename
        
        rows = []
        
        # Process each window result
        if hasattr(_wf_result, 'window_results'):
            for wr in _wf_result.window_results:
                row = {
                    'window_id': wr.window.window_id if hasattr(wr, 'window') else '',
                    'train_start': wr.window.train_start if hasattr(wr, 'window') else '',
                    'train_end': wr.window.train_end if hasattr(wr, 'window') else '',
                    'test_start': wr.window.test_start if hasattr(wr, 'window') else '',
                    'test_end': wr.window.test_end if hasattr(wr, 'window') else '',
                }
                
                # Add best params (ensure _trade_dir always present)
                if hasattr(wr, 'best_params') and wr.best_params:
                    bp = dict(wr.best_params)
                    bp.setdefault('_trade_dir', 'long_short')
                    for key, value in bp.items():
                        row[f'param_{key}'] = value
                
                # Add train metrics
                if hasattr(wr, 'train_result') and wr.train_result:
                    row['train_net_profit'] = wr.train_result.net_profit
                    row['train_profit_factor'] = wr.train_result.profit_factor
                    row['train_sharpe'] = wr.train_result.sharpe_ratio
                    row['train_max_dd_pct'] = wr.train_result.max_drawdown_pct
                
                # Add test metrics
                if hasattr(wr, 'test_result') and wr.test_result:
                    row['test_net_profit'] = wr.test_result.net_profit
                    row['test_profit_factor'] = wr.test_result.profit_factor
                    row['test_sharpe'] = wr.test_result.sharpe_ratio
                    row['test_max_dd_pct'] = wr.test_result.max_drawdown_pct
                    row['test_win_rate'] = wr.test_result.win_rate
                    row['test_trades'] = wr.test_result.total_trades
                
                rows.append(row)
        
        df = pd.DataFrame(rows)
        
        # Add aggregated summary row
        if len(df) > 0 and hasattr(_wf_result, 'net_profit_mean'):
            summary = {
                'window_id': 'AGGREGATED',
                'test_net_profit': _wf_result.net_profit_mean,
                'test_profit_factor': _wf_result.profit_factor_mean,
                'test_sharpe': getattr(_wf_result, 'sharpe_mean', 0),
                'test_max_dd_pct': getattr(_wf_result, 'max_dd_mean', 0),
                'test_win_rate': getattr(_wf_result, 'win_rate_mean', 0),
            }
            df = pd.concat([df, pd.DataFrame([summary])], ignore_index=True)
        
        df.to_csv(output_path, index=False)
        return output_path
    
    # Alias for export_walk_forward
    def export_walk_forward_results(
        self,
        _wf_result: Any,
        _filename: str = "walk_forward.csv",
    ) -> Path:
        """Alias for export_walk_forward for backward compatibility."""
        return self.export_walk_forward(_wf_result, _filename)
    
    def export_optimization_results(
        self,
        _opt_result: Any,
        _filename: str = "optimization.csv",
    ) -> Path:
        """
        Export optimization results to CSV.
        
        Args:
            _opt_result: OptimizationResult object
            _filename: Output filename
        
        Returns:
            Path to exported file
        """
        output_path = self._output_dir / _filename
        
        # Use to_dataframe if available
        if hasattr(_opt_result, 'to_dataframe'):
            df = _opt_result.to_dataframe()
        else:
            # Manual conversion
            rows = []
            if hasattr(_opt_result, 'all_results'):
                for params, result in _opt_result.all_results:
                    row = params.copy()
                    row['net_profit'] = result.net_profit
                    row['profit_factor'] = result.profit_factor
                    row['sharpe_ratio'] = result.sharpe_ratio
                    row['max_drawdown_pct'] = result.max_drawdown_pct
                    row['win_rate'] = result.win_rate
                    row['total_trades'] = result.total_trades
                    rows.append(row)
            df = pd.DataFrame(rows)
        
        df.to_csv(output_path, index=False)
        return output_path
    
    def export_portfolio(
        self,
        _strategies: List[Dict[str, Any]],
        _allocations: List[float],
        _filename: str = "portfolio.csv",
    ) -> Path:
        """
        Export portfolio allocation to CSV.
        
        Args:
            _strategies: List of strategy info dicts
            _allocations: Allocation weights
            _filename: Output filename
        
        Returns:
            Path to exported file
        """
        output_path = self._output_dir / _filename
        
        rows = []
        for strat, alloc in zip(_strategies, _allocations):
            row = {
                'strategy_name': strat.get('name', ''),
                'instrument': strat.get('instrument', ''),
                'timeframe': strat.get('timeframe', ''),
                'allocation_weight': alloc,
                'version': strat.get('version', 1),
            }
            # Add strategy parameters
            params = strat.get('params', {})
            for key, value in params.items():
                row[f'param_{key}'] = value
            rows.append(row)
        
        df = pd.DataFrame(rows)
        df.to_csv(output_path, index=False)
        return output_path
    
    def _create_trade_summary(self, _trades_df: pd.DataFrame) -> Dict[str, Any]:
        """Create summary row for trades export."""
        summary = {
            'trade_id': 'SUMMARY',
            'symbol': '',
            'direction': '',
            'entry_time': '',
            'entry_price': '',
            'exit_time': '',
            'exit_price': '',
            'quantity': _trades_df['quantity'].sum() if 'quantity' in _trades_df.columns else 0,
            'stop_loss': '',
            'take_profit': '',
            'pnl': _trades_df['pnl'].sum() if 'pnl' in _trades_df.columns else 0,
            'commission': _trades_df['commission'].sum() if 'commission' in _trades_df.columns else 0,
            'slippage': _trades_df['slippage'].sum() if 'slippage' in _trades_df.columns else 0,
            'exit_reason': f"Total: {len(_trades_df)} trades",
            'mae': _trades_df['mae'].mean() if 'mae' in _trades_df.columns else 0,
            'mfe': _trades_df['mfe'].mean() if 'mfe' in _trades_df.columns else 0,
            'bars_held': _trades_df['bars_held'].mean() if 'bars_held' in _trades_df.columns else 0,
        }
        return summary
