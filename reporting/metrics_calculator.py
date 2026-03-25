"""
Metrics Calculator Module
=========================
Performance metrics calculation for trading strategies.

Metrics (matching MT5 standard set + additional):
    - Sharpe ratio
    - Sortino ratio
    - Calmar ratio
    - Win rate
    - Average win/loss
    - Consecutive wins/losses
    - Time in market
    - Profit factor
    - Recovery factor
    - Max drawdown
    - MAE/MFE analysis
    - Ulcer Index
    - UPI (Ulcer Performance Index) with duration scaling
    - Efficiency ratio with proportionality
    - Consistency score with proportionality
    - Stability factor
    - Average risk-adjusted monthly return

Classes:
    MetricsCalculator
        - calculate_all
        - calculate_sharpe
        - calculate_sortino
        - calculate_calmar
        - calculate_profit_factor
        - calculate_recovery_factor
        - calculate_max_drawdown
        - calculate_win_stats
        - calculate_consecutive_stats
        - calculate_mae_mfe_stats
        - calculate_ulcer_index
        - calculate_upi (_source_duration, _target_duration, _apply_scaling)
        - calculate_efficiency_ratio (_is_duration, _oos_duration, _apply_proportionality)
        - calculate_consistency_score (_is_duration, _oos_duration, _apply_proportionality)
        - calculate_stability_factor
        - calculate_avg_risk_adj_monthly_return (net_profit / num_months / max_drawdown)
        - calculate_scaled_metric

Usage:
    calc = MetricsCalculator(_risk_free_rate=0.02, _annualization_factor=252)
    metrics = calc.calculate_all(_trades=trades, _equity_curve=equity)
    sharpe = calc.calculate_sharpe(_returns=returns)
    sortino = calc.calculate_sortino(_returns=returns)
    upi = calc.calculate_upi(_net_profit=1000, _equity_curve=equity, _apply_scaling=True)
    efficiency = calc.calculate_efficiency_ratio(_is_metric=2.0, _oos_metric=1.8, _apply_proportionality=True)
    avg_risk_adj = calc.calculate_avg_risk_adj_monthly_return(_net_profit=10000, _num_months=12, _max_drawdown=5000)
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple
import pandas as pd
import numpy as np
from datetime import datetime


@dataclass
class PerformanceMetrics:
    """Container for all performance metrics."""
    # Return metrics
    net_profit: float = 0.0
    gross_profit: float = 0.0
    gross_loss: float = 0.0
    
    # Risk-adjusted returns
    sharpe_ratio: float = 0.0
    sortino_ratio: float = 0.0
    calmar_ratio: float = 0.0
    
    # Win/loss stats
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    win_rate: float = 0.0
    avg_win: float = 0.0
    avg_loss: float = 0.0
    largest_win: float = 0.0
    largest_loss: float = 0.0
    
    # Consecutive stats
    max_consecutive_wins: int = 0
    max_consecutive_losses: int = 0
    
    # Risk metrics
    profit_factor: float = 0.0
    recovery_factor: float = 0.0
    max_drawdown: float = 0.0
    max_drawdown_pct: float = 0.0
    max_drawdown_duration: int = 0
    
    # Time metrics
    avg_bars_held: float = 0.0
    time_in_market: float = 0.0
    
    # Cost metrics
    total_commission: float = 0.0
    total_slippage: float = 0.0
    cost_pct_of_pnl: float = 0.0
    
    # Trade quality
    avg_mae: float = 0.0
    avg_mfe: float = 0.0
    mae_mfe_ratio: float = 0.0
    
    # Advanced metrics (Phase 3c)
    avg_risk_adj_monthly_return: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'net_profit': self.net_profit,
            'gross_profit': self.gross_profit,
            'gross_loss': self.gross_loss,
            'sharpe_ratio': self.sharpe_ratio,
            'sortino_ratio': self.sortino_ratio,
            'calmar_ratio': self.calmar_ratio,
            'total_trades': self.total_trades,
            'winning_trades': self.winning_trades,
            'losing_trades': self.losing_trades,
            'win_rate': self.win_rate,
            'avg_win': self.avg_win,
            'avg_loss': self.avg_loss,
            'largest_win': self.largest_win,
            'largest_loss': self.largest_loss,
            'max_consecutive_wins': self.max_consecutive_wins,
            'max_consecutive_losses': self.max_consecutive_losses,
            'profit_factor': self.profit_factor,
            'recovery_factor': self.recovery_factor,
            'max_drawdown': self.max_drawdown,
            'max_drawdown_pct': self.max_drawdown_pct,
            'max_drawdown_duration': self.max_drawdown_duration,
            'avg_bars_held': self.avg_bars_held,
            'time_in_market': self.time_in_market,
            'total_commission': self.total_commission,
            'total_slippage': self.total_slippage,
            'cost_pct_of_pnl': self.cost_pct_of_pnl,
            'avg_mae': self.avg_mae,
            'avg_mfe': self.avg_mfe,
            'mae_mfe_ratio': self.mae_mfe_ratio,
            'avg_risk_adj_monthly_return': self.avg_risk_adj_monthly_return,
        }


class MetricsCalculator:
    """
    Performance metrics calculator.
    
    Calculates standard metrics matching MT5 set plus additional
    risk-adjusted and trade quality metrics.
    """
    
    def __init__(
        self,
        _risk_free_rate: float = 0.02,
        _annualization_factor: int = 252,
    ):
        """
        Initialize calculator.
        
        Args:
            _risk_free_rate: Annual risk-free rate (default 2%)
            _annualization_factor: Trading days per year (default 252)
        """
        self._risk_free_rate = _risk_free_rate
        self._annualization_factor = _annualization_factor
    
    def calculate_all(
        self,
        _trades: List[Dict[str, Any]],
        _equity_curve: pd.Series,
        _initial_capital: float = 100000.0,
        _total_bars: int = 0,
        _bars_per_day: int = 0,
    ) -> PerformanceMetrics:
        """
        Calculate all performance metrics.
        
        Args:
            _trades: List of trade dictionaries
            _equity_curve: Equity curve series
            _initial_capital: Starting capital
            _total_bars: Total bars in backtest
            _bars_per_day: Bars per trading day (e.g., 96 for M15, 24 for H1).
                           If 0 or not provided, auto-detected from equity curve index,
                           falling back to _annualization_factor (daily returns assumed).
        
        Returns:
            PerformanceMetrics with all calculated values
        """
        metrics = PerformanceMetrics()
        
        if not _trades:
            return metrics
        
        # Convert to DataFrame for easier calculation
        trades_df = pd.DataFrame(_trades)
        
        # Basic profit metrics
        pnls = trades_df['pnl'].values if 'pnl' in trades_df.columns else np.array([])
        wins = pnls[pnls > 0]
        losses = pnls[pnls < 0]
        
        metrics.net_profit = float(np.sum(pnls))
        metrics.gross_profit = float(np.sum(wins)) if len(wins) > 0 else 0.0
        metrics.gross_loss = float(np.abs(np.sum(losses))) if len(losses) > 0 else 0.0
        
        # Win/loss stats
        metrics.total_trades = len(_trades)
        metrics.winning_trades = len(wins)
        metrics.losing_trades = len(losses)
        metrics.win_rate = (metrics.winning_trades / metrics.total_trades * 100) if metrics.total_trades > 0 else 0.0
        metrics.avg_win = float(np.mean(wins)) if len(wins) > 0 else 0.0
        metrics.avg_loss = float(np.mean(losses)) if len(losses) > 0 else 0.0
        metrics.largest_win = float(np.max(wins)) if len(wins) > 0 else 0.0
        metrics.largest_loss = float(np.min(losses)) if len(losses) > 0 else 0.0
        
        # Consecutive stats
        cons_wins, cons_losses = self.calculate_consecutive_stats(pnls)
        metrics.max_consecutive_wins = cons_wins
        metrics.max_consecutive_losses = cons_losses
        
        # Profit factor
        metrics.profit_factor = self.calculate_profit_factor(metrics.gross_profit, metrics.gross_loss)
        
        # Drawdown metrics
        dd, dd_pct, dd_duration = self.calculate_max_drawdown(_equity_curve, _initial_capital)
        metrics.max_drawdown = dd
        metrics.max_drawdown_pct = dd_pct
        metrics.max_drawdown_duration = dd_duration
        
        # Recovery factor
        metrics.recovery_factor = self.calculate_recovery_factor(metrics.net_profit, metrics.max_drawdown)
        
        # Determine annualization factor for bar-level returns.
        # For intraday data (M15 = 96 bars/day), using the daily factor (252) on
        # per-bar returns causes the per-bar risk-free rate subtraction to dominate
        # near-zero returns, producing misleadingly negative Sharpe/Sortino values
        # even for profitable strategies. The fix: annualize based on actual bars/year.
        bars_annualization = self._annualization_factor  # default: daily (252)
        if _bars_per_day > 0:
            bars_annualization = self._annualization_factor * _bars_per_day
        elif isinstance(_equity_curve.index, pd.DatetimeIndex) and len(_equity_curve) > 2:
            # Auto-detect from median bar interval
            diffs = _equity_curve.index.to_series().diff().dropna()
            if len(diffs) > 0:
                median_minutes = diffs.dt.total_seconds().median() / 60.0
                if 0 < median_minutes < 1440:
                    estimated_bars_per_day = int(round(1440 / median_minutes))
                    bars_annualization = self._annualization_factor * estimated_bars_per_day
        
        # Risk-adjusted returns from equity curve
        returns = _equity_curve.pct_change().dropna()
        metrics.sharpe_ratio = self.calculate_sharpe(returns, _annualization_override=bars_annualization)
        metrics.sortino_ratio = self.calculate_sortino(returns, _annualization_override=bars_annualization)
        metrics.calmar_ratio = self.calculate_calmar(metrics.net_profit, _initial_capital, metrics.max_drawdown_pct)
        
        # Time metrics
        if 'bars_held' in trades_df.columns:
            metrics.avg_bars_held = float(trades_df['bars_held'].mean())
            total_bars_held = trades_df['bars_held'].sum()
            metrics.time_in_market = (total_bars_held / _total_bars * 100) if _total_bars > 0 else 0.0
        
        # Cost metrics
        if 'commission' in trades_df.columns:
            metrics.total_commission = float(trades_df['commission'].sum())
        if 'slippage' in trades_df.columns:
            metrics.total_slippage = float(trades_df['slippage'].sum())
        total_cost = metrics.total_commission + metrics.total_slippage
        metrics.cost_pct_of_pnl = (total_cost / abs(metrics.net_profit) * 100) if metrics.net_profit != 0 else 0.0
        
        # Trade quality (MAE/MFE)
        if 'mae' in trades_df.columns and 'mfe' in trades_df.columns:
            metrics.avg_mae = float(trades_df['mae'].mean())
            metrics.avg_mfe = float(trades_df['mfe'].mean())
            metrics.mae_mfe_ratio = metrics.avg_mae / metrics.avg_mfe if metrics.avg_mfe != 0 else 0.0
        
        return metrics
    
    def calculate_sharpe(self, _returns: pd.Series, _annualization_override: int = 0) -> float:
        """
        Calculate Sharpe ratio.
        
        Formula: (mean_return - risk_free_per_bar) / std_return * sqrt(annualization)
        
        The risk-free rate is divided by the annualization factor to get the per-bar
        risk-free rate. For intraday data (e.g. M15), the annualization factor should
        be 252 * bars_per_day so that the per-bar risk-free subtraction is proportionally
        small and does not dominate near-zero bar returns.
        
        Args:
            _returns: Return series (per-bar pct_change of equity curve)
            _annualization_override: If > 0, use this instead of self._annualization_factor
        
        Returns:
            Sharpe ratio
        """
        if _returns.empty or _returns.std() == 0:
            return 0.0
        
        ann = _annualization_override if _annualization_override > 0 else self._annualization_factor
        excess_returns = _returns - (self._risk_free_rate / ann)
        sharpe = (excess_returns.mean() / _returns.std()) * np.sqrt(ann)
        
        return float(sharpe) if not np.isnan(sharpe) else 0.0
    
    def calculate_sortino(self, _returns: pd.Series, _annualization_override: int = 0) -> float:
        """
        Calculate Sortino ratio (uses downside deviation).
        
        Args:
            _returns: Return series (per-bar pct_change of equity curve)
            _annualization_override: If > 0, use this instead of self._annualization_factor
        
        Returns:
            Sortino ratio
        """
        if _returns.empty:
            return 0.0
        
        downside_returns = _returns[_returns < 0]
        downside_std = downside_returns.std() if len(downside_returns) > 0 else 0
        
        if downside_std == 0:
            return 0.0
        
        ann = _annualization_override if _annualization_override > 0 else self._annualization_factor
        excess_return = _returns.mean() - (self._risk_free_rate / ann)
        sortino = (excess_return / downside_std) * np.sqrt(ann)
        
        return float(sortino) if not np.isnan(sortino) else 0.0
    
    def calculate_calmar(
        self,
        _net_profit: float,
        _initial_capital: float,
        _max_dd_pct: float,
    ) -> float:
        """
        Calculate Calmar ratio (annual return / max drawdown).
        
        Args:
            _net_profit: Net profit
            _initial_capital: Starting capital
            _max_dd_pct: Max drawdown percentage
        
        Returns:
            Calmar ratio
        """
        if _max_dd_pct == 0:
            return 0.0
        
        annual_return_pct = (_net_profit / _initial_capital) * 100
        calmar = annual_return_pct / abs(_max_dd_pct)
        
        return float(calmar) if not np.isnan(calmar) else 0.0
    
    def calculate_profit_factor(
        self,
        _gross_profit: float,
        _gross_loss: float,
    ) -> float:
        """
        Calculate profit factor (gross profit / gross loss).
        
        Args:
            _gross_profit: Total profits
            _gross_loss: Total losses (positive number)
        
        Returns:
            Profit factor
        """
        if _gross_loss == 0:
            return float('inf') if _gross_profit > 0 else 0.0
        return _gross_profit / _gross_loss
    
    def calculate_recovery_factor(
        self,
        _net_profit: float,
        _max_drawdown: float,
    ) -> float:
        """
        Calculate recovery factor (net profit / max drawdown).
        
        Args:
            _net_profit: Net profit
            _max_drawdown: Max drawdown (absolute value)
        
        Returns:
            Recovery factor
        """
        if _max_drawdown == 0:
            return 0.0
        return _net_profit / abs(_max_drawdown)
    
    def calculate_max_drawdown(
        self,
        _equity_curve: pd.Series,
        _initial_capital: float = 100000.0,
    ) -> Tuple[float, float, int]:
        """
        Calculate maximum drawdown and duration.
        
        Args:
            _equity_curve: Equity curve series
            _initial_capital: Starting capital
        
        Returns:
            Tuple of (max_drawdown_absolute, max_drawdown_pct, max_duration_bars)
        """
        if _equity_curve.empty:
            return 0.0, 0.0, 0
        
        # Calculate running maximum
        rolling_max = _equity_curve.expanding().max()
        drawdown = rolling_max - _equity_curve
        drawdown_pct = (drawdown / rolling_max) * 100
        
        max_dd = float(drawdown.max())
        max_dd_pct = float(drawdown_pct.max())
        
        # Calculate duration
        in_drawdown = drawdown > 0
        duration = 0
        max_duration = 0
        
        for is_dd in in_drawdown:
            if is_dd:
                duration += 1
                max_duration = max(max_duration, duration)
            else:
                duration = 0
        
        return max_dd, max_dd_pct, max_duration
    
    def calculate_consecutive_stats(self, _pnls: np.ndarray) -> Tuple[int, int]:
        """
        Calculate max consecutive wins and losses.
        
        Args:
            _pnls: Array of PnL values
        
        Returns:
            Tuple of (max_consecutive_wins, max_consecutive_losses)
        """
        if len(_pnls) == 0:
            return 0, 0
        
        max_wins = 0
        max_losses = 0
        current_wins = 0
        current_losses = 0
        
        for pnl in _pnls:
            if pnl > 0:
                current_wins += 1
                current_losses = 0
                max_wins = max(max_wins, current_wins)
            elif pnl < 0:
                current_losses += 1
                current_wins = 0
                max_losses = max(max_losses, current_losses)
            else:  # pnl == 0
                current_wins = 0
                current_losses = 0
        
        return max_wins, max_losses
    
    def calculate_expectancy(
        self,
        _win_rate: float,
        _avg_win: float,
        _avg_loss: float,
    ) -> float:
        """
        Calculate trade expectancy (expected value per trade).
        
        Args:
            _win_rate: Win rate as percentage (0-100)
            _avg_win: Average winning trade
            _avg_loss: Average losing trade (negative number)
        
        Returns:
            Expected value per trade
        """
        win_prob = _win_rate / 100
        lose_prob = 1 - win_prob
        return (win_prob * _avg_win) + (lose_prob * _avg_loss)
    
    # =========================================================================
    # UPI (ULCER PERFORMANCE INDEX) CALCULATIONS
    # =========================================================================
    
    def calculate_ulcer_index(self, _equity_curve: pd.Series) -> float:
        """
        Calculate Ulcer Index (measure of downside volatility/risk).
        
        The Ulcer Index measures the depth and duration of drawdowns.
        Lower values indicate less risk.
        
        Formula:
            UI = sqrt(mean(sum of squared percentage drawdowns))
        
        Args:
            _equity_curve: Equity curve series
        
        Returns:
            Ulcer Index value
        """
        if _equity_curve.empty or len(_equity_curve) < 2:
            return 0.0
        
        # Calculate rolling maximum (peak)
        rolling_max = _equity_curve.expanding().max()
        
        # Calculate percentage drawdown from peak
        pct_drawdown = ((rolling_max - _equity_curve) / rolling_max) * 100
        
        # Square the drawdowns
        squared_drawdowns = pct_drawdown ** 2
        
        # Calculate Ulcer Index
        ulcer_index = np.sqrt(squared_drawdowns.mean())
        
        return float(ulcer_index) if not np.isnan(ulcer_index) else 0.0
    
    def calculate_upi(
        self,
        _net_profit: float,
        _equity_curve: pd.Series,
        _initial_capital: float = 100000.0,
        _source_duration: float = 1.0,
        _target_duration: float = 1.0,
        _apply_scaling: bool = False,
    ) -> float:
        """
        Calculate Ulcer Performance Index (UPI).
        
        UPI = Net Profit / Ulcer Index
        
        Higher UPI indicates better risk-adjusted performance.
        Rewards returns while penalizing drawdown depth and duration.
        
        Since net_profit in numerator is duration-dependent, UPI should be scaled
        when comparing periods of different lengths (like net_profit scaling).
        
        Args:
            _net_profit: Net profit (in currency)
            _equity_curve: Equity curve series
            _initial_capital: Starting capital
            _source_duration: Duration of source period (months, bars, or ratio)
            _target_duration: Duration of target period (same units as source)
            _apply_scaling: If True, scale UPI by duration ratio
        
        Returns:
            UPI value (optionally scaled)
            
        Note:
            - UPI scales like net_profit because net_profit is in numerator
            - Scaling formula: UPI_scaled = UPI * (target_duration / source_duration)
            - Use for fair comparison across periods of different lengths
        """
        ulcer_index = self.calculate_ulcer_index(_equity_curve)
        
        if ulcer_index == 0:
            return float('inf') if _net_profit > 0 else 0.0
        
        # Normalize net profit as percentage return
        pct_return = (_net_profit / _initial_capital) * 100
        
        upi = pct_return / ulcer_index
        
        # Apply duration scaling if requested
        if _apply_scaling and _source_duration > 0:
            duration_ratio = _target_duration / _source_duration
            upi = upi * duration_ratio
        
        return float(upi) if not np.isnan(upi) else 0.0
    
    # =========================================================================
    # VALIDATION METRICS (EFFICIENCY, CONSISTENCY, STABILITY)
    # =========================================================================
    
    def calculate_efficiency_ratio(
        self,
        _is_metric: float,
        _oos_metric: float,
        _is_duration: float = 1.0,
        _oos_duration: float = 1.0,
        _apply_proportionality: bool = True,
    ) -> float:
        """
        Calculate efficiency ratio (OOS metric / IS metric).
        
        Measures how well in-sample performance translates to out-of-sample.
        Ratio close to 1.0 indicates robust strategy.
        
        With proportionality adjustment for metrics that scale with time:
            Adjusted OOS metric = OOS metric * (IS duration / OOS duration)
        
        Args:
            _is_metric: In-sample metric value
            _oos_metric: Out-of-sample metric value
            _is_duration: IS period duration (in months or bars)
            _oos_duration: OOS period duration (same units as IS)
            _apply_proportionality: If True, adjust for period duration differences
        
        Returns:
            Efficiency ratio (OOS/IS)
        """
        if _is_metric == 0 or _is_metric == float('inf'):
            return 0.0
        
        oos_adjusted = _oos_metric
        
        # Apply proportionality adjustment for time-dependent metrics
        # (e.g., net profit, number of trades scale with duration)
        if _apply_proportionality and _oos_duration > 0:
            duration_ratio = _is_duration / _oos_duration
            oos_adjusted = _oos_metric * duration_ratio
        
        efficiency = oos_adjusted / _is_metric
        
        return float(efficiency) if not np.isnan(efficiency) else 0.0
    
    def calculate_consistency_score(
        self,
        _is_metric: float,
        _oos_metric: float,
        _alpha: float = 0.5,
        _is_duration: float = 1.0,
        _oos_duration: float = 1.0,
        _apply_proportionality: bool = True,
    ) -> float:
        """
        Calculate consistency score that rewards OOS performance and penalizes degradation.
        
        Formula:
            oos_adjusted = OOS_metric * (IS_duration / OOS_duration) if proportionality applied
            degradation = |IS_metric - oos_adjusted| / IS_metric
            consistency_factor = 1 - alpha * degradation
            score = oos_adjusted * consistency_factor
        
        Higher alpha = more penalty for degradation (typical range: 0.5-1.0)
        
        Args:
            _is_metric: In-sample metric value (e.g., profit factor from IS2)
            _oos_metric: Out-of-sample metric value
            _alpha: Penalty weight for degradation (default 0.5)
            _is_duration: IS period duration (in months, bars, or ratio)
            _oos_duration: OOS period duration (same units as IS)
            _apply_proportionality: If True, adjust OOS metric for period duration differences
        
        Returns:
            Consistency score
            
        Note:
            - With proportionality, OOS metric is scaled to make fair comparison
            - Matches signature of calculate_efficiency_ratio for consistent API
            - Use for time-dependent metrics (net_profit, UPI, trade count)
        """
        if _is_metric <= 0:
            return 0.0
        
        # Apply proportionality adjustment if requested
        oos_adjusted = _oos_metric
        if _apply_proportionality and _oos_duration > 0:
            duration_ratio = _is_duration / _oos_duration
            oos_adjusted = _oos_metric * duration_ratio
        
        degradation = abs(_is_metric - oos_adjusted) / _is_metric
        consistency_factor = 1 - _alpha * degradation
        
        # Ensure consistency factor is non-negative
        consistency_factor = max(0, consistency_factor)
        
        score = oos_adjusted * consistency_factor
        
        return float(score) if not np.isnan(score) else 0.0
    
    def calculate_stability_factor(
        self,
        _base_metric: float,
        _metric_values: List[float],
        _tolerance_pct: float = 10.0,
    ) -> float:
        """
        Calculate stability factor based on metric variance within tolerance.
        
        Measures what percentage of metric values fall within ±tolerance% of base metric.
        
        Args:
            _base_metric: Reference metric value (e.g., from IS1)
            _metric_values: List of metric values to check (e.g., from IS2, OOS windows)
            _tolerance_pct: Tolerance percentage (default ±10%)
        
        Returns:
            Stability factor (0.0 to 1.0)
        """
        if not _metric_values or _base_metric == 0:
            return 0.0
        
        lower_bound = _base_metric * (1 - _tolerance_pct / 100)
        upper_bound = _base_metric * (1 + _tolerance_pct / 100)
        
        within_tolerance = sum(
            1 for v in _metric_values 
            if lower_bound <= v <= upper_bound
        )
        
        stability = within_tolerance / len(_metric_values)
        
        return float(stability)
    
    def calculate_avg_risk_adj_monthly_return(
        self,
        _net_profit: float,
        _num_months: float,
        _max_drawdown: float,
    ) -> float:
        """
        Calculate average risk-adjusted monthly return.
        
        Formula:
            avg_risk_adj_monthly_return = net_profit / num_months / max_drawdown
        
        This metric combines:
            - Return magnitude (net_profit)
            - Time efficiency (per month)
            - Risk consideration (divided by max_drawdown)
        
        Higher values indicate better risk-adjusted performance per unit of time.
        
        Args:
            _net_profit: Total net profit over the period (in currency)
            _num_months: Number of months in the period (can be float)
            _max_drawdown: Maximum drawdown (in currency, positive value)
        
        Returns:
            Average risk-adjusted monthly return
            
        Note:
            - Returns 0.0 if num_months or max_drawdown is zero/near-zero
            - max_drawdown should be positive (absolute value)
            - Useful for comparing strategies with different evaluation periods
        """
        if _num_months <= 0 or _max_drawdown <= 0:
            return 0.0
        
        avg_monthly_return = _net_profit / _num_months
        risk_adj_return = avg_monthly_return / _max_drawdown
        
        return float(risk_adj_return) if not np.isnan(risk_adj_return) else 0.0
    
    def calculate_scaled_metric(
        self,
        _metric_value: float,
        _source_duration: float,
        _target_duration: float,
        _is_ratio_metric: bool = False,
    ) -> float:
        """
        Scale a metric from one period duration to another.
        
        For additive metrics (net profit, trades): scale proportionally
        For ratio metrics (profit factor, sharpe): no scaling needed
        
        Args:
            _metric_value: Original metric value
            _source_duration: Source period duration
            _target_duration: Target period duration
            _is_ratio_metric: If True, metric is a ratio (no scaling)
        
        Returns:
            Scaled metric value
        """
        if _is_ratio_metric or _source_duration == 0:
            return _metric_value
        
        scale_factor = _target_duration / _source_duration
        return _metric_value * scale_factor
    
    def aggregate_walk_forward_metrics(
        self,
        _window_results: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        Aggregate metrics across walk-forward windows.
        
        Calculates mean, std, min, max for key metrics across all OOS periods.
        
        Args:
            _window_results: List of window result dicts with 'test_result' keys
        
        Returns:
            Aggregated metrics dictionary
        """
        if not _window_results:
            return {}
        
        # Extract test results
        test_results = []
        for wr in _window_results:
            if 'test_result' in wr and wr['test_result'] is not None:
                test_results.append(wr['test_result'])
        
        if not test_results:
            return {}
        
        # Key metrics to aggregate
        metrics_keys = [
            'net_profit', 'profit_factor', 'recovery_factor', 'sharpe_ratio',
            'sortino_ratio', 'calmar_ratio', 'max_drawdown_pct', 'win_rate',
            'total_trades', 'avg_win', 'avg_loss', 'avg_bars_held'
        ]
        
        aggregated = {
            'num_windows': len(test_results),
        }
        
        for key in metrics_keys:
            values = []
            for tr in test_results:
                # Handle both BacktestResult objects and dicts
                if hasattr(tr, key):
                    val = getattr(tr, key)
                elif isinstance(tr, dict) and key in tr:
                    val = tr[key]
                else:
                    continue
                
                if val is not None and not (isinstance(val, float) and (np.isnan(val) or np.isinf(val))):
                    values.append(val)
            
            if values:
                aggregated[f'{key}_mean'] = float(np.mean(values))
                aggregated[f'{key}_std'] = float(np.std(values))
                aggregated[f'{key}_min'] = float(np.min(values))
                aggregated[f'{key}_max'] = float(np.max(values))
                aggregated[f'{key}_median'] = float(np.median(values))
        
        return aggregated
    
    def combine_equity_curves(
        self,
        _window_results: List[Dict[str, Any]],
        _initial_capital: float = 100000.0,
    ) -> Tuple[List[float], List[datetime]]:
        """
        Combine equity curves from walk-forward windows into single curve.
        
        Each window's equity is normalized and chained to previous window's end.
        
        Args:
            _window_results: List of window results with equity curves
            _initial_capital: Starting capital
        
        Returns:
            Tuple of (combined_equity, combined_timestamps)
        """
        combined_equity = [_initial_capital]
        combined_timestamps = []
        current_capital = _initial_capital
        
        for wr in _window_results:
            test_result = wr.get('test_result')
            if test_result is None:
                continue
            
            # Get equity curve - handle both object and dict
            if hasattr(test_result, 'equity_curve'):
                equity = test_result.equity_curve
                timestamps = getattr(test_result, 'equity_timestamps', [])
            elif isinstance(test_result, dict):
                equity = test_result.get('equity_curve', [])
                timestamps = test_result.get('equity_timestamps', [])
            else:
                continue
            
            if not equity or len(equity) < 2:
                continue
            
            # Normalize to start from current capital
            start_equity = equity[0] if equity[0] != 0 else 1
            for i, eq in enumerate(equity[1:], 1):
                normalized = current_capital * (eq / start_equity)
                combined_equity.append(normalized)
                if i < len(timestamps):
                    combined_timestamps.append(timestamps[i])
            
            # Update current capital to end of this window
            if equity:
                current_capital = combined_equity[-1]
        
        return combined_equity, combined_timestamps
    
    def calculate_robustness_score(
        self,
        _aggregated_metrics: Dict[str, Any],
    ) -> float:
        """
        Calculate robustness score based on consistency across windows.
        
        Higher score = more consistent performance across OOS periods.
        Score range: 0.0 to 1.0
        
        Args:
            _aggregated_metrics: Output from aggregate_walk_forward_metrics
        
        Returns:
            Robustness score (0-1)
        """
        if not _aggregated_metrics:
            return 0.0
        
        scores = []
        
        # Profit factor consistency
        pf_mean = _aggregated_metrics.get('profit_factor_mean', 0)
        pf_std = _aggregated_metrics.get('profit_factor_std', 1)
        if pf_mean > 0 and pf_std > 0:
            pf_cv = pf_std / pf_mean  # Coefficient of variation
            pf_score = max(0, 1 - pf_cv)  # Lower CV = higher score
            scores.append(pf_score)
        
        # Win rate consistency
        wr_mean = _aggregated_metrics.get('win_rate_mean', 0)
        wr_std = _aggregated_metrics.get('win_rate_std', 1)
        if wr_mean > 0 and wr_std > 0:
            wr_cv = wr_std / wr_mean
            wr_score = max(0, 1 - wr_cv)
            scores.append(wr_score)
        
        # Positive windows ratio
        num_windows = _aggregated_metrics.get('num_windows', 1)
        net_profit_min = _aggregated_metrics.get('net_profit_min', 0)
        if num_windows > 0:
            # Check if minimum profit is positive (all windows profitable)
            profitable_score = 1.0 if net_profit_min > 0 else 0.5
            scores.append(profitable_score)
        
        # Drawdown consistency
        dd_mean = _aggregated_metrics.get('max_drawdown_pct_mean', 100)
        dd_max = _aggregated_metrics.get('max_drawdown_pct_max', 100)
        if dd_max > 0:
            dd_ratio = dd_mean / dd_max  # Closer to 1 = more consistent
            scores.append(dd_ratio)
        
        return float(np.mean(scores)) if scores else 0.0
    
    def calculate_from_backtest_result(
        self,
        _backtest_result: Any,
    ) -> PerformanceMetrics:
        """
        Calculate metrics from a BacktestResult object.
        
        Convenience method to convert BacktestResult to PerformanceMetrics.
        
        Args:
            _backtest_result: BacktestResult object or dict
        
        Returns:
            PerformanceMetrics object
        """
        # Handle both object and dict input
        if hasattr(_backtest_result, 'trades'):
            trades = [t.to_dict() if hasattr(t, 'to_dict') else t for t in _backtest_result.trades]
            equity = _backtest_result.equity_curve if hasattr(_backtest_result, 'equity_curve') else []
            initial = _backtest_result.initial_capital if hasattr(_backtest_result, 'initial_capital') else 100000
            total_bars = len(equity) if equity else 0
        elif isinstance(_backtest_result, dict):
            trades = _backtest_result.get('trades', [])
            equity = _backtest_result.get('equity_curve', [])
            initial = _backtest_result.get('initial_capital', 100000)
            total_bars = len(equity)
        else:
            return PerformanceMetrics()
        
        equity_series = pd.Series(equity) if equity else pd.Series([initial])
        
        return self.calculate_all(
            _trades=trades,
            _equity_curve=equity_series,
            _initial_capital=initial,
            _total_bars=total_bars,
        )
    
    def generate_summary_report(
        self,
        _metrics: PerformanceMetrics,
        _strategy_name: str = "",
        _symbol: str = "",
        _timeframe: str = "",
    ) -> str:
        """
        Generate text summary report of performance metrics.
        
        Args:
            _metrics: PerformanceMetrics object
            _strategy_name: Strategy name for header
            _symbol: Symbol for header
            _timeframe: Timeframe for header
        
        Returns:
            Formatted text report
        """
        lines = [
            "=" * 60,
            f"PERFORMANCE REPORT: {_strategy_name}",
            f"Symbol: {_symbol} | Timeframe: {_timeframe}",
            "=" * 60,
            "",
            "RETURNS",
            "-" * 40,
            f"  Net Profit:      ${_metrics.net_profit:,.2f}",
            f"  Gross Profit:    ${_metrics.gross_profit:,.2f}",
            f"  Gross Loss:      ${_metrics.gross_loss:,.2f}",
            "",
            "RISK-ADJUSTED RETURNS",
            "-" * 40,
            f"  Sharpe Ratio:    {_metrics.sharpe_ratio:.2f}",
            f"  Sortino Ratio:   {_metrics.sortino_ratio:.2f}",
            f"  Calmar Ratio:    {_metrics.calmar_ratio:.2f}",
            "",
            "TRADE STATISTICS",
            "-" * 40,
            f"  Total Trades:    {_metrics.total_trades}",
            f"  Win Rate:        {_metrics.win_rate:.1f}%",
            f"  Winning Trades:  {_metrics.winning_trades}",
            f"  Losing Trades:   {_metrics.losing_trades}",
            f"  Avg Win:         ${_metrics.avg_win:,.2f}",
            f"  Avg Loss:        ${_metrics.avg_loss:,.2f}",
            f"  Largest Win:     ${_metrics.largest_win:,.2f}",
            f"  Largest Loss:    ${_metrics.largest_loss:,.2f}",
            f"  Max Consec Wins: {_metrics.max_consecutive_wins}",
            f"  Max Consec Loss: {_metrics.max_consecutive_losses}",
            "",
            "RISK METRICS",
            "-" * 40,
            f"  Profit Factor:   {_metrics.profit_factor:.2f}",
            f"  Recovery Factor: {_metrics.recovery_factor:.2f}",
            f"  Max Drawdown:    ${_metrics.max_drawdown:,.2f}",
            f"  Max Drawdown %:  {_metrics.max_drawdown_pct:.1f}%",
            "",
            "TIME METRICS",
            "-" * 40,
            f"  Avg Bars Held:   {_metrics.avg_bars_held:.1f}",
            f"  Time in Market:  {_metrics.time_in_market:.1f}%",
            "",
            "COST ANALYSIS",
            "-" * 40,
            f"  Total Commission: ${_metrics.total_commission:,.2f}",
            f"  Total Slippage:   ${_metrics.total_slippage:,.2f}",
            f"  Cost % of PnL:    {_metrics.cost_pct_of_pnl:.1f}%",
            "",
            "TRADE QUALITY",
            "-" * 40,
            f"  Avg MAE:         ${_metrics.avg_mae:,.4f}",
            f"  Avg MFE:         ${_metrics.avg_mfe:,.4f}",
            f"  MAE/MFE Ratio:   {_metrics.mae_mfe_ratio:.2f}",
            "",
            "=" * 60,
        ]
        
        return "\n".join(lines)
