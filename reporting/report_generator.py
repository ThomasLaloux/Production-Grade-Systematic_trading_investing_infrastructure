"""
Report Generator Module
=======================
PDF report generation and visualization for backtest results.

Classes:
    ReportGenerator
        - generate_backtest_report
        - generate_walk_forward_report
        - plot_equity_curve
        - plot_drawdown
        - plot_monthly_returns
        - plot_trade_distribution

Usage:
    reporter = ReportGenerator(_output_dir="./reports")
    reporter.plot_equity_curve(_equity=equity, _timestamps=timestamps, _filename="equity.jpg")
    reporter.plot_drawdown(_equity=equity, _filename="drawdown.jpg")
    reporter.generate_backtest_report(_result=result, _filename="report.pdf", _title="My Strategy")
"""

from pathlib import Path
from typing import Any, Dict, List, Optional, Union, Tuple
from datetime import datetime
import pandas as pd
import numpy as np

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

# Import matplotlib with non-interactive backend for server environments
try:
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    from matplotlib.backends.backend_pdf import PdfPages
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False


class ReportGenerator:
    """
    Report generator for backtest results visualization.
    
    Creates equity curves, drawdown charts, and PDF reports.
    """
    
    def __init__(self, _output_dir: str = "./reports"):
        """
        Initialize report generator.
        
        Args:
            _output_dir: Output directory for reports
        """
        self._output_dir = Path(_output_dir)
        self._output_dir.mkdir(parents=True, exist_ok=True)
        
        if not MATPLOTLIB_AVAILABLE:
            print("Warning: matplotlib not available. Visualization disabled.")
    
    def plot_equity_curve(
        self,
        _equity: Union[pd.Series, List[float]],
        _timestamps: Optional[Union[pd.Series, List]] = None,
        _balance: Optional[Union[pd.Series, List[float]]] = None,
        _filename: str = "equity_curve.jpg",
        _title: str = "Equity Curve",
        _initial_capital: float = 100000.0,
        _figsize: Tuple[int, int] = (14, 6),
    ) -> Optional[Path]:
        """
        Plot equity curve with optional balance curve.
        
        Args:
            _equity: Equity values (green line)
            _timestamps: Optional timestamps
            _balance: Optional balance values (blue line)
            _filename: Output filename
            _title: Chart title
            _initial_capital: Starting capital (for horizontal line)
            _figsize: Figure size
        
        Returns:
            Path to saved file or None if matplotlib not available
        """
        if not MATPLOTLIB_AVAILABLE:
            return None
        
        output_path = self._output_dir / _filename
        
        fig, ax = plt.subplots(figsize=_figsize)
        
        if _timestamps is not None:
            if _balance is not None and len(_balance) == len(_equity):
                ax.plot(_timestamps, _balance, linewidth=1.5, color='#2563EB', label='Balance', alpha=0.85)
            ax.plot(_timestamps, _equity, linewidth=1.5, color='#16A34A', label='Equity')
            ax.axhline(y=_initial_capital, color='gray', linestyle='--', alpha=0.5, label='Initial Capital')
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
            ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
            plt.xticks(rotation=45)
        else:
            if _balance is not None and len(_balance) == len(_equity):
                ax.plot(_balance, linewidth=1.5, color='#2563EB', label='Balance', alpha=0.85)
            ax.plot(_equity, linewidth=1.5, color='#16A34A', label='Equity')
            ax.axhline(y=_initial_capital, color='gray', linestyle='--', alpha=0.5, label='Initial Capital')
            ax.set_xlabel('Bar')
        
        ax.set_ylabel('Equity ($)')
        ax.set_title(_title)
        ax.legend(loc='upper left')
        ax.grid(True, alpha=0.3)
        
        # Y-axis on both sides
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.0f}'))
        ax.tick_params(axis='y', labelright=True, right=True)
        
        plt.tight_layout()
        # Detect format from extension for JPG compatibility
        file_ext = output_path.suffix.lower()
        save_kwargs = {'dpi': 150, 'bbox_inches': 'tight', 'facecolor': 'white', 'edgecolor': 'none'}
        if file_ext in ('.jpg', '.jpeg'):
            save_kwargs['format'] = 'jpeg'
        plt.savefig(output_path, **save_kwargs)
        plt.close()
        
        return output_path
    
    def plot_drawdown(
        self,
        _equity: Union[pd.Series, List[float]],
        _timestamps: Optional[Union[pd.Series, List]] = None,
        _filename: str = "drawdown.jpg",
        _title: str = "Drawdown",
        _figsize: Tuple[int, int] = (14, 4),
    ) -> Optional[Path]:
        """
        Plot drawdown chart.
        
        Args:
            _equity: Equity values
            _timestamps: Optional timestamps
            _filename: Output filename
            _title: Chart title
            _figsize: Figure size
        
        Returns:
            Path to saved file
        """
        if not MATPLOTLIB_AVAILABLE:
            return None
        
        output_path = self._output_dir / _filename
        
        # Calculate drawdown
        equity_series = pd.Series(_equity)
        rolling_max = equity_series.expanding().max()
        drawdown = (rolling_max - equity_series) / rolling_max * 100
        
        fig, ax = plt.subplots(figsize=_figsize)
        
        if _timestamps is not None:
            ax.fill_between(_timestamps, 0, -drawdown, color='red', alpha=0.4)
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
            ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
            plt.xticks(rotation=45)
        else:
            ax.fill_between(range(len(drawdown)), 0, -drawdown, color='red', alpha=0.4)
            ax.set_xlabel('Bar')
        
        ax.set_ylabel('Drawdown (%)')
        ax.set_title(_title)
        ax.grid(True, alpha=0.3)
        ax.tick_params(axis='y', labelright=True, right=True)
        
        plt.tight_layout()
        # Detect format from extension for JPG compatibility
        file_ext = output_path.suffix.lower()
        save_kwargs = {'dpi': 150, 'bbox_inches': 'tight', 'facecolor': 'white', 'edgecolor': 'none'}
        if file_ext in ('.jpg', '.jpeg'):
            save_kwargs['format'] = 'jpeg'
        plt.savefig(output_path, **save_kwargs)
        plt.close()
        
        return output_path
    
    def plot_monthly_returns(
        self,
        _equity: Union[pd.Series, List[float]],
        _timestamps: Union[pd.Series, List],
        _filename: str = "monthly_returns.jpg",
        _title: str = "Monthly Returns",
        _figsize: Tuple[int, int] = (12, 6),
    ) -> Optional[Path]:
        """
        Plot monthly returns heatmap.
        
        Args:
            _equity: Equity values
            _timestamps: Timestamps
            _filename: Output filename
            _title: Chart title
            _figsize: Figure size
        
        Returns:
            Path to saved file
        """
        if not MATPLOTLIB_AVAILABLE:
            return None
        
        output_path = self._output_dir / _filename
        
        # Create DataFrame with timestamps
        df = pd.DataFrame({'equity': _equity, 'timestamp': pd.to_datetime(_timestamps)})
        df.set_index('timestamp', inplace=True)
        
        # Calculate monthly returns
        monthly = df['equity'].resample('ME').last()
        monthly_returns = monthly.pct_change() * 100
        
        # Create pivot table for heatmap
        monthly_returns = monthly_returns.dropna()
        if len(monthly_returns) == 0:
            return None
        
        returns_df = pd.DataFrame({
            'year': monthly_returns.index.year,
            'month': monthly_returns.index.month,
            'return': monthly_returns.values
        })
        
        pivot = returns_df.pivot(index='year', columns='month', values='return')
        
        # Create heatmap
        fig, ax = plt.subplots(figsize=_figsize)
        
        # Custom colormap
        cmap = plt.cm.RdYlGn
        
        im = ax.imshow(pivot.values, cmap=cmap, aspect='auto', vmin=-10, vmax=10)
        
        # Set ticks
        ax.set_xticks(range(12))
        ax.set_xticklabels(['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                           'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'])
        ax.set_yticks(range(len(pivot.index)))
        ax.set_yticklabels(pivot.index)
        
        # Add colorbar
        cbar = plt.colorbar(im, ax=ax)
        cbar.set_label('Return (%)')
        
        # Add text annotations
        for i in range(len(pivot.index)):
            for j in range(12):
                if j + 1 in pivot.columns:
                    val = pivot.iloc[i, pivot.columns.get_loc(j + 1)]
                    if not np.isnan(val):
                        ax.text(j, i, f'{val:.1f}', ha='center', va='center',
                               color='white' if abs(val) > 5 else 'black', fontsize=8)
        
        ax.set_title(_title)
        plt.tight_layout()
        # Detect format from extension for JPG compatibility
        file_ext = output_path.suffix.lower()
        save_kwargs = {'dpi': 150, 'bbox_inches': 'tight', 'facecolor': 'white', 'edgecolor': 'none'}
        if file_ext in ('.jpg', '.jpeg'):
            save_kwargs['format'] = 'jpeg'
        plt.savefig(output_path, **save_kwargs)
        plt.close()
        
        return output_path
    
    def plot_trade_distribution(
        self,
        _trades: List[Dict[str, Any]],
        _filename: str = "trade_distribution.jpg",
        _title: str = "Trade P&L Distribution",
        _figsize: Tuple[int, int] = (12, 6),
    ) -> Optional[Path]:
        """
        Plot trade P&L distribution histogram.
        
        Args:
            _trades: List of trade dictionaries
            _filename: Output filename
            _title: Chart title
            _figsize: Figure size
        
        Returns:
            Path to saved file
        """
        if not MATPLOTLIB_AVAILABLE or not _trades:
            return None
        
        output_path = self._output_dir / _filename
        
        # Extract PnLs - handle both dict and object
        pnls = []
        for t in _trades:
            if isinstance(t, dict):
                pnls.append(t.get('pnl', 0))
            elif hasattr(t, 'pnl'):
                pnls.append(t.pnl)
        
        if not pnls:
            return None
        
        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p <= 0]
        
        fig, ax = plt.subplots(figsize=_figsize)
        
        # Plot histograms
        if wins:
            ax.hist(wins, bins=20, alpha=0.7, color='green', label=f'Wins ({len(wins)})')
        if losses:
            ax.hist(losses, bins=20, alpha=0.7, color='red', label=f'Losses ({len(losses)})')
        
        ax.axvline(x=0, color='black', linestyle='-', linewidth=1)
        if pnls:
            ax.axvline(x=np.mean(pnls), color='blue', linestyle='--', label=f'Mean: ${np.mean(pnls):.2f}')
        
        ax.set_xlabel('P&L ($)')
        ax.set_ylabel('Frequency')
        ax.set_title(_title)
        ax.legend()
        ax.grid(True, alpha=0.3)
        
        plt.tight_layout()
        # Detect format from extension for JPG compatibility
        file_ext = output_path.suffix.lower()
        save_kwargs = {'dpi': 150, 'bbox_inches': 'tight', 'facecolor': 'white', 'edgecolor': 'none'}
        if file_ext in ('.jpg', '.jpeg'):
            save_kwargs['format'] = 'jpeg'
        plt.savefig(output_path, **save_kwargs)
        plt.close()
        
        return output_path
    
    def generate_backtest_report(
        self,
        _result: Any,
        _filename: str = "backtest_report.pdf",
        _title: str = "",
        _equity: Optional[Union[pd.Series, List[float]]] = None,
        _timestamps: Optional[Union[pd.Series, List]] = None,
        _trades: Optional[List[Dict[str, Any]]] = None,
    ) -> Optional[Path]:
        """
        Generate comprehensive backtest PDF report.
        
        Args:
            _result: BacktestResult object
            _filename: Output filename
            _title: Report title (optional, overrides strategy name)
            _equity: Equity curve (optional, extracted from result if not provided)
            _timestamps: Timestamps (optional)
            _trades: Trade list (optional, extracted from result if not provided)
        
        Returns:
            Path to saved file
        """
        if not MATPLOTLIB_AVAILABLE:
            return None
        
        output_path = self._output_dir / _filename
        file_ext = output_path.suffix.lower()
        
        # Extract data from result if not provided
        if _equity is None and hasattr(_result, 'equity_curve'):
            _equity = _result.equity_curve
        if _timestamps is None and hasattr(_result, 'equity_timestamps'):
            _timestamps = _result.equity_timestamps
        if _trades is None and hasattr(_result, 'trades'):
            # Convert Trade objects to dicts if needed
            _trades = []
            for t in _result.trades:
                if hasattr(t, 'to_dict'):
                    _trades.append(t.to_dict())
                elif isinstance(t, dict):
                    _trades.append(t)
                else:
                    _trades.append({
                        'pnl': getattr(t, 'pnl', 0),
                        'trade_id': getattr(t, 'trade_id', ''),
                    })
        
        # Detect format: image (jpg/png) vs PDF
        is_image_format = file_ext in ('.jpg', '.jpeg', '.png')
        
        # Get metrics from result
        if hasattr(_result, 'to_dict'):
            metrics = _result.to_dict()
        else:
            metrics = vars(_result) if hasattr(_result, '__dict__') else {}
        
        # Use custom title if provided
        report_title = _title if _title else metrics.get('strategy_name', 'N/A')
        
        # Calculate avg risk-adj monthly return
        arm_text = ""
        sd = metrics.get('start_date')
        ed = metrics.get('end_date')
        if sd and ed:
            try:
                days = (pd.to_datetime(ed) - pd.to_datetime(sd)).days
                num_months = days / 30.44
                md = metrics.get('max_drawdown', 0)
                np_ = metrics.get('net_profit', 0)
                if num_months > 0 and md > 0:
                    arm = np_ / num_months / md
                    arm_text = f"\nAvg Risk-Adj Monthly: {arm:.4f}"
            except Exception:
                pass
        
        # Build summary text
        summary_text = f"""
BACKTEST REPORT
{'='*50}

Strategy: {report_title}
Symbol: {metrics.get('symbol', 'N/A')}
Timeframe: {metrics.get('timeframe', 'N/A')}
Period: {metrics.get('start_date', 'N/A')} to {metrics.get('end_date', 'N/A')}

PERFORMANCE SUMMARY
{'-'*50}
Net Profit: ${metrics.get('net_profit', 0):,.2f}
Profit Factor: {metrics.get('profit_factor', 0):.2f}
Sharpe Ratio: {metrics.get('sharpe_ratio', 0):.2f}
Sortino Ratio: {metrics.get('sortino_ratio', 0):.2f}
Calmar Ratio: {metrics.get('calmar_ratio', 0):.2f}
Recovery Factor: {metrics.get('recovery_factor', 0):.2f}{arm_text}

TRADE STATISTICS
{'-'*50}
Total Trades: {metrics.get('total_trades', 0)}
Winning Trades: {metrics.get('winning_trades', 0)}
Losing Trades: {metrics.get('losing_trades', 0)}
Win Rate: {metrics.get('win_rate', 0):.1f}%
Average Win: ${metrics.get('avg_win', 0):,.2f}
Average Loss: ${metrics.get('avg_loss', 0):,.2f}
Largest Win: ${metrics.get('largest_win', 0):,.2f}
Largest Loss: ${metrics.get('largest_loss', 0):,.2f}

RISK METRICS
{'-'*50}
Max Drawdown: ${metrics.get('max_drawdown', 0):,.2f}
Max Drawdown %: {metrics.get('max_drawdown_pct', 0):.2f}%
Max Consecutive Wins: {metrics.get('max_consecutive_wins', 0)}
Max Consecutive Losses: {metrics.get('max_consecutive_losses', 0)}

COSTS
{'-'*50}
Total Commission: ${metrics.get('total_commission', 0):,.2f}
Total Slippage: ${metrics.get('total_slippage', 0):,.2f}
"""
        
        if is_image_format:
            # Single-image output (jpg/png) - all panels in one figure
            n_panels = 1 + (1 if _equity else 0) + (1 if _trades else 0)
            fig, axes = plt.subplots(n_panels, 1, figsize=(14, 6 * n_panels))
            if n_panels == 1:
                axes = [axes]
            
            # Panel 1: Summary text
            axes[0].axis('off')
            axes[0].text(0.05, 0.95, summary_text, transform=axes[0].transAxes, fontsize=9,
                        verticalalignment='top', fontfamily='monospace')
            
            panel_idx = 1
            
            # Panel 2: Equity curve + drawdown
            if _equity is not None and len(_equity) > 0 and panel_idx < n_panels:
                ax_eq = axes[panel_idx]
                x_vals = range(len(_equity))
                if _timestamps is not None and len(_timestamps) == len(_equity):
                    x_vals = _timestamps
                
                # Balance (blue) + Equity (green)
                _balance_img = None
                if hasattr(_result, 'balance_curve') and _result.balance_curve:
                    _balance_img = _result.balance_curve
                if _balance_img and len(_balance_img) == len(_equity):
                    ax_eq.plot(x_vals, _balance_img, color='#2563EB', linewidth=1, label='Balance', alpha=0.85)
                ax_eq.plot(x_vals, _equity, color='#16A34A', linewidth=1, label='Equity')
                ax_eq.set_title(f'{report_title} - Balance & Equity Curve')
                ax_eq.set_ylabel('Value ($)')
                ax_eq.grid(True, alpha=0.3)
                ax_eq.legend(loc='upper left')
                ax_eq.tick_params(axis='y', labelright=True, right=True)
                panel_idx += 1
            
            # Panel 3: Trade P&L distribution
            if _trades and panel_idx < n_panels:
                pnls = [t.get('pnl', 0) for t in _trades if isinstance(t, dict)]
                if pnls:
                    ax_pnl = axes[panel_idx]
                    colors = ['green' if p > 0 else 'red' for p in pnls]
                    ax_pnl.bar(range(len(pnls)), pnls, color=colors, alpha=0.7)
                    ax_pnl.set_title('Trade P&L Distribution')
                    ax_pnl.set_ylabel('P&L ($)')
                    ax_pnl.axhline(y=0, color='black', linewidth=0.5)
                    ax_pnl.grid(True, alpha=0.3)
            
            plt.tight_layout()
            
            # Save with correct format (white background for JPEG compatibility)
            fmt = 'jpeg' if file_ext in ('.jpg', '.jpeg') else 'png'
            fig.patch.set_facecolor('white')
            for ax in axes:
                ax.set_facecolor('white')
            plt.savefig(output_path, format=fmt, dpi=150, bbox_inches='tight',
                       facecolor='white', edgecolor='none')
            plt.close()
            
        else:
            # PDF output - multi-page
            with PdfPages(output_path) as pdf:
                # Page 1: Summary
                fig, ax = plt.subplots(figsize=(10, 8))
                ax.axis('off')
                ax.text(0.1, 0.95, summary_text, transform=ax.transAxes, fontsize=10,
                       verticalalignment='top', fontfamily='monospace')
                pdf.savefig(fig)
                plt.close()
                
                # Page 2: Equity curve
                if _equity is not None and len(_equity) > 0:
                    fig, axes = plt.subplots(2, 1, figsize=(14, 10))
                    
                    # Balance + Equity curve
                    ax1 = axes[0]
                    _balance = None
                    if hasattr(_result, 'balance_curve') and _result.balance_curve:
                        _balance = _result.balance_curve
                    
                    if _timestamps is not None:
                        if _balance and len(_balance) == len(_equity):
                            ax1.plot(_timestamps, _balance, linewidth=1.5, color='#2563EB', label='Balance', alpha=0.85)
                        ax1.plot(_timestamps, _equity, linewidth=1.5, color='#16A34A', label='Equity')
                        ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
                    else:
                        if _balance and len(_balance) == len(_equity):
                            ax1.plot(_balance, linewidth=1.5, color='#2563EB', label='Balance', alpha=0.85)
                        ax1.plot(_equity, linewidth=1.5, color='#16A34A', label='Equity')
                    ax1.set_ylabel('Equity ($)')
                    ax1.set_title('Balance & Equity Curve')
                    ax1.legend(loc='upper left')
                    ax1.grid(True, alpha=0.3)
                    ax1.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.0f}'))
                    ax1.tick_params(axis='y', labelright=True, right=True)
                    
                    # Drawdown
                    ax2 = axes[1]
                    equity_series = pd.Series(_equity)
                    rolling_max = equity_series.expanding().max()
                    drawdown = (rolling_max - equity_series) / rolling_max * 100
                    
                    if _timestamps is not None:
                        ax2.fill_between(_timestamps, 0, -drawdown, color='red', alpha=0.5)
                        ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
                    else:
                        ax2.fill_between(range(len(drawdown)), 0, -drawdown, color='red', alpha=0.5)
                    ax2.set_ylabel('Drawdown (%)')
                    ax2.set_title('Drawdown')
                    ax2.grid(True, alpha=0.3)
                    
                    plt.tight_layout()
                    pdf.savefig(fig)
                    plt.close()
                
                # Page 3: Trade distribution
                if _trades:
                    fig, axes = plt.subplots(2, 1, figsize=(12, 10))
                    
                    pnls = [t.get('pnl', 0) if isinstance(t, dict) else getattr(t, 'pnl', 0) for t in _trades]
                    
                    # P&L histogram
                    ax1 = axes[0]
                    wins = [p for p in pnls if p > 0]
                    losses = [p for p in pnls if p <= 0]
                    if wins:
                        ax1.hist(wins, bins=20, alpha=0.7, color='green', label=f'Wins ({len(wins)})')
                    if losses:
                        ax1.hist(losses, bins=20, alpha=0.7, color='red', label=f'Losses ({len(losses)})')
                    ax1.axvline(x=0, color='black', linestyle='-', linewidth=1)
                    if pnls:
                        ax1.axvline(x=np.mean(pnls), color='blue', linestyle='--', label=f'Mean: ${np.mean(pnls):.2f}')
                    ax1.set_xlabel('P&L ($)')
                    ax1.set_ylabel('Frequency')
                    ax1.set_title('Trade P&L Distribution')
                    ax1.legend()
                    ax1.grid(True, alpha=0.3)
                    
                    # Cumulative P&L
                    ax2 = axes[1]
                    cumulative_pnl = np.cumsum(pnls)
                    ax2.plot(cumulative_pnl, linewidth=1.5, color='blue')
                    ax2.axhline(y=0, color='black', linestyle='-', linewidth=0.5)
                    ax2.set_xlabel('Trade #')
                    ax2.set_ylabel('Cumulative P&L ($)')
                    ax2.set_title('Cumulative P&L by Trade')
                    ax2.grid(True, alpha=0.3)
                    ax2.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.0f}'))
                    
                    plt.tight_layout()
                    pdf.savefig(fig)
                    plt.close()
        
        return output_path
    
    def generate_walk_forward_report(
        self,
        _wf_result: Any,
        _filename: str = "walk_forward_report.pdf",
        _title: str = "",
    ) -> Optional[Path]:
        """
        Generate walk-forward analysis report.
        
        Supports two output formats based on file extension:
            - Image (.jpg/.jpeg/.png): Single-page with summary text, equity curve,
              and per-window profit bars (same layout as generate_backtest_report)
            - PDF (.pdf): Multi-page detailed report
                Page 1: Same 3-panel layout as the image output
                Page 2: Per-window details table
                Page 3: Combined OOS equity curve + drawdown (full-page)
                Page 4: Per-window profit bar chart (full-page)
        
        Args:
            _wf_result: WalkForwardResult object
            _filename: Output filename (.jpg/.png for image, .pdf for multi-page)
            _title: Report title (optional, overrides strategy name)
        
        Returns:
            Path to saved file
        """
        if not MATPLOTLIB_AVAILABLE:
            return None
        
        output_path = self._output_dir / _filename
        file_ext = output_path.suffix.lower()
        is_image_format = file_ext in ('.jpg', '.jpeg', '.png')
        
        # Use custom title if provided
        report_title = _title if _title else getattr(_wf_result, 'strategy_name', 'N/A')
        
        # ----------------------------------------------------------------
        # Extract all metrics (shared between image and PDF)
        # ----------------------------------------------------------------
        np_mean = getattr(_wf_result, 'oos_net_profit_mean', 0)
        np_std = getattr(_wf_result, 'oos_net_profit_std', 0)
        pf_mean = getattr(_wf_result, 'oos_profit_factor_mean', 0)
        rf_mean = getattr(_wf_result, 'oos_recovery_factor_mean', 0)
        sharpe_mean = getattr(_wf_result, 'oos_sharpe_mean', 0)
        dd_mean = getattr(_wf_result, 'oos_max_dd_mean', 0)
        wr_mean = getattr(_wf_result, 'oos_win_rate_mean', 0)
        
        np_agg = getattr(_wf_result, 'net_profit_agg', 0)
        pf_agg = getattr(_wf_result, 'profit_factor_agg', 0)
        rf_agg = getattr(_wf_result, 'recovery_factor_agg', 0)
        dd_agg = getattr(_wf_result, 'max_drawdown_agg', 0)
        wr_agg = getattr(_wf_result, 'win_rate_agg', 0)
        sharpe_agg = getattr(_wf_result, 'sharpe_agg', 0)
        trades_agg = getattr(_wf_result, 'total_trades_agg', 0)
        gp_agg = getattr(_wf_result, 'gross_profit_agg', 0)
        gl_agg = getattr(_wf_result, 'gross_loss_agg', 0)
        
        num_windows = len(getattr(_wf_result, 'window_results', []))
        is_ratio = getattr(_wf_result, 'is_ratio', 0)
        oos_ratio = getattr(_wf_result, 'oos_ratio', 0)
        method = getattr(_wf_result, 'method', 'N/A')
        optim_metric = getattr(_wf_result, 'optimization_metric', 'N/A')
        best_params = getattr(_wf_result, 'best_params', {})
        if best_params and isinstance(best_params, dict):
            best_params = dict(best_params)
            best_params.setdefault('_trade_dir', 'long_short')
        
        params_text = "N/A"
        if best_params and isinstance(best_params, dict):
            params_text = ", ".join(f"{k}: {v}" for k, v in best_params.items())
        
        summary_text = f"""
WALK-FORWARD REPORT
{'='*50}

Strategy: {report_title}
Symbol: {getattr(_wf_result, 'symbol', 'N/A')}
Timeframe: {getattr(_wf_result, 'timeframe', 'N/A')}
Method: {method}  |  IS/OOS: {is_ratio:.0%}/{oos_ratio:.0%}
Metric: {optim_metric}  |  Windows: {num_windows}

OOS METRICS (Mean across windows)
{'-'*50}
Net Profit (mean):     ${np_mean:>10,.2f} (std: ${np_std:,.2f})
Profit Factor (mean):  {pf_mean:>10.2f}
Recovery Factor (mean):{rf_mean:>10.2f}
Sharpe Ratio (mean):   {sharpe_mean:>10.2f}
Max Drawdown % (mean): {dd_mean:>9.1f}%
Win Rate (mean):       {wr_mean:>9.1f}%

AGGREGATED OOS (all windows combined)
{'-'*50}
Net Profit (total):    ${np_agg:>10,.2f}
Profit Factor (agg):   {pf_agg:>10.2f}
Recovery Factor (agg): {rf_agg:>10.2f}
Max Drawdown % (worst):{dd_agg:>9.1f}%
Win Rate (agg):        {wr_agg:>9.1f}%
Total Trades:          {trades_agg:>10}

Best Params: {params_text}
"""
        
        # Extract equity data
        equity = getattr(_wf_result, 'combined_equity', None)
        timestamps = getattr(_wf_result, 'combined_timestamps', None)
        balance = getattr(_wf_result, 'combined_balance', None)
        has_equity = equity is not None and len(equity) > 0
        
        # Extract per-window profits
        window_profits = []
        if hasattr(_wf_result, 'window_results') and _wf_result.window_results:
            for wr in _wf_result.window_results:
                if hasattr(wr, 'test_result') and wr.test_result:
                    window_profits.append(wr.test_result.net_profit)
        has_window_bars = len(window_profits) > 0
        
        # ----------------------------------------------------------------
        # Helper: build the 3-panel figure (shared between image and PDF p1)
        # ----------------------------------------------------------------
        def _build_summary_figure():
            """Build the 3-panel summary figure (text + equity + per-window bars)."""
            n_panels = 1 + (1 if has_equity else 0) + (1 if has_window_bars else 0)
            fig, axes = plt.subplots(n_panels, 1, figsize=(14, 6 * n_panels))
            if n_panels == 1:
                axes = [axes]
            
            # Panel 1: Summary text
            axes[0].axis('off')
            axes[0].text(0.05, 0.95, summary_text, transform=axes[0].transAxes, fontsize=9,
                        verticalalignment='top', fontfamily='monospace')
            
            panel_idx = 1
            
            # Panel 2: Combined OOS equity curve (balance blue + equity green)
            if has_equity and panel_idx < n_panels:
                ax_eq = axes[panel_idx]
                x_vals = range(len(equity))
                if timestamps is not None and len(timestamps) == len(equity):
                    x_vals = timestamps
                
                if balance and len(balance) == len(equity):
                    ax_eq.plot(x_vals, balance, color='#2563EB', linewidth=1, label='Balance', alpha=0.85)
                ax_eq.plot(x_vals, equity, color='#16A34A', linewidth=1, label='Equity')
                ax_eq.set_title(f'{report_title} - Combined OOS Balance & Equity')
                ax_eq.set_ylabel('Value ($)')
                ax_eq.grid(True, alpha=0.3)
                ax_eq.legend(loc='upper left')
                ax_eq.tick_params(axis='y', labelright=True, right=True)
                panel_idx += 1
            
            # Panel 3: Per-window profit bars
            if has_window_bars and panel_idx < n_panels:
                ax_bar = axes[panel_idx]
                colors = ['#16A34A' if p > 0 else '#DC2626' for p in window_profits]
                ax_bar.bar(range(len(window_profits)), window_profits, color=colors, alpha=0.7)
                ax_bar.set_title('Per-Window OOS Net Profit')
                ax_bar.set_xlabel('Window')
                ax_bar.set_ylabel('Net Profit ($)')
                ax_bar.axhline(y=0, color='black', linewidth=0.5)
                ax_bar.grid(True, alpha=0.3, axis='y')
            
            plt.tight_layout()
            fig.patch.set_facecolor('white')
            for ax in axes:
                ax.set_facecolor('white')
            return fig
        
        # ----------------------------------------------------------------
        # IMAGE output (.jpg/.png) — single page, same as backtest report
        # ----------------------------------------------------------------
        if is_image_format:
            fig = _build_summary_figure()
            fmt = 'jpeg' if file_ext in ('.jpg', '.jpeg') else 'png'
            plt.savefig(output_path, format=fmt, dpi=150, bbox_inches='tight',
                       facecolor='white', edgecolor='none')
            plt.close()
        
        # ----------------------------------------------------------------
        # PDF output — multi-page: page 1 = same as image, pages 2+ = details
        # ----------------------------------------------------------------
        else:
            with PdfPages(output_path) as pdf:
                # Page 1: Same 3-panel summary as image output
                fig = _build_summary_figure()
                pdf.savefig(fig, facecolor='white')
                plt.close()
                
                # Page 2: Per-window details table
                if hasattr(_wf_result, 'window_results') and _wf_result.window_results:
                    fig, ax = plt.subplots(figsize=(11, 8.5))
                    ax.axis('off')
                    
                    header = f"PER-WINDOW OOS DETAILS\n{'='*90}\n"
                    header += f"{'Win':<5} {'Train Period':<24} {'Test Period':<24} {'Net Profit':>12} {'PF':>7} {'WR':>6} {'MaxDD':>7} {'Trades':>7}\n"
                    header += f"{'-'*90}\n"
                    
                    lines = [header]
                    for i, wr in enumerate(_wf_result.window_results):
                        window = wr.window
                        tr = wr.test_result
                        
                        train_p = f"{window.train_start.date()} - {window.train_end.date()}"
                        test_p = f"{window.test_start.date()} - {window.test_end.date()}"
                        
                        net_p = tr.net_profit if tr else 0
                        pf = tr.profit_factor if tr else 0
                        wr_pct = tr.win_rate if tr else 0
                        mdd = tr.max_drawdown_pct if tr else 0
                        trades = tr.total_trades if tr else 0
                        
                        pf_s = f"{pf:>6.2f}" if pf != float('inf') else "   inf"
                        lines.append(f"{i:<5} {train_p:<24} {test_p:<24} ${net_p:>10,.0f} {pf_s} {wr_pct:>5.1f}% {mdd:>5.1f}% {trades:>7}")
                    
                    lines.append(f"{'='*90}")
                    
                    ax.text(0.03, 0.97, "\n".join(lines), transform=ax.transAxes, fontsize=8.5,
                           verticalalignment='top', fontfamily='monospace')
                    pdf.savefig(fig, facecolor='white')
                    plt.close()
                
                # Page 3: Combined OOS equity curve + drawdown (full-page)
                if has_equity:
                    fig, axes = plt.subplots(2, 1, figsize=(14, 10),
                                            gridspec_kw={'height_ratios': [2, 1], 'hspace': 0.15})
                    
                    ax1 = axes[0]
                    x_vals = range(len(equity))
                    if timestamps is not None and len(timestamps) == len(equity):
                        x_vals = timestamps
                    if balance and len(balance) == len(equity):
                        ax1.plot(x_vals, balance, linewidth=1.5, color='#2563EB', label='Balance', alpha=0.85)
                    ax1.plot(x_vals, equity, linewidth=1.5, color='#16A34A', label='Equity')
                    ax1.set_ylabel('Value ($)')
                    ax1.set_title(f'{report_title} — Combined OOS Equity')
                    ax1.legend(loc='upper left')
                    ax1.grid(True, alpha=0.3)
                    ax1.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.0f}'))
                    ax1.tick_params(axis='y', labelright=True, right=True)
                    
                    # Drawdown
                    ax2 = axes[1]
                    equity_series = pd.Series(equity)
                    rolling_max = equity_series.expanding().max()
                    drawdown = (rolling_max - equity_series) / rolling_max * 100
                    
                    if timestamps is not None and len(timestamps) == len(drawdown):
                        ax2.fill_between(timestamps, 0, -drawdown, color='red', alpha=0.25)
                    else:
                        ax2.fill_between(range(len(drawdown)), 0, -drawdown, color='red', alpha=0.25)
                    ax2.set_ylabel('Drawdown (%)')
                    ax2.set_title('Relative Drawdown')
                    ax2.grid(True, alpha=0.3)
                    ax2.tick_params(axis='y', labelright=True, right=True)
                    
                    plt.tight_layout()
                    pdf.savefig(fig, facecolor='white')
                    plt.close()
                
                # Page 4: Per-window profit bar chart (full-page)
                if has_window_bars:
                    fig, ax = plt.subplots(figsize=(14, 6))
                    colors = ['#16A34A' if p > 0 else '#DC2626' for p in window_profits]
                    ax.bar(range(len(window_profits)), window_profits, color=colors, alpha=0.8)
                    ax.axhline(y=0, color='black', linestyle='-', linewidth=0.5)
                    ax.set_xlabel('Window')
                    ax.set_ylabel('Net Profit ($)')
                    ax.set_title(f'{report_title} — Per-Window OOS Net Profit')
                    ax.grid(True, alpha=0.3, axis='y')
                    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.0f}'))
                    
                    plt.tight_layout()
                    pdf.savefig(fig, facecolor='white')
                    plt.close()
        
        return output_path
