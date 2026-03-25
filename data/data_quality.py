"""
Data Quality Module
===================
Data quality checks and visualization.

Gap detection logic (Phase 5.11):
    Expected gaps are suppressed from the terminal report:
      - Nightly session breaks: defined by per-broker, per-asset-class
        market_hours in brokers.yaml.  The gap is expected when it falls
        inside the break window between two intraday sessions.
      - Weekend gaps: Friday close → Sunday/Monday open (as before).
      - US market holidays: auto-generated static calendar covers
        New Year, MLK Day, Presidents Day, Good Friday, Memorial Day,
        Independence Day, Labor Day, Thanksgiving, Christmas
        for the years present in the data.
    Only genuinely unexpected intraday gaps are reported.

Classes:
    QualityReport(symbol, timeframe, total_bars, date_range, issues, passed, summary_stats):
        - summary of data quality checks
    DataQualityChecker:
        - validates OHLCV data and flags quality issues
        - run_all_checks
        - check_price_spikes
        - check_timestamp_gaps
        - check_volume_anomalies
        - get_summary_report
        - to_dataframe
        - get_summary
        - plot_report
        - visualize_issues

Usage:
    checker = DataQualityChecker()
    checker = DataQualityChecker(_spike_threshold=5.0, _gap_multiplier=2.0, _volume_multiplier=100.0)
    report = checker.run_all_checks(df, "EURUSD", "M1")
    issues = checker.check_price_spikes(df)
    issues = checker.check_timestamp_gaps(df, "M1")
    issues = checker.check_volume_anomalies(df)
    text = checker.get_summary_report(report)
    df_issues = checker.to_dataframe(report)                 # DataFrame with all issues
    summary = checker.get_summary(report)                    # Summary dict with counts and score
    checker.plot_report(df, report, "quality_report.png")    # Visual report with traffic light
    checker.visualize_issues(df, report, "quality_report.png")
"""

from pathlib import Path
from datetime import datetime, date, time, timedelta
from typing import List, Dict, Any, Optional, Tuple, Set
from dataclasses import dataclass
import pandas as pd
import numpy as np

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.data_types import Timeframe, DataQualityIssue


# ---------------------------------------------------------------------------
# Asset-class mapping: instruments.yaml names → brokers.yaml market_hours keys
# ---------------------------------------------------------------------------
_ASSET_CLASS_TO_MARKET_HOURS_KEY = {
    "forex": "forex",
    "commodity": "metals",
    "metals": "metals",
    "index": "indices",
    "index futures": "indices",
    "indices": "indices",
    "crypto": None,           # 24/7, no session breaks
}


# ---------------------------------------------------------------------------
# US market holidays — static generator
# ---------------------------------------------------------------------------

def _nth_weekday_of_month(year: int, month: int, weekday: int, n: int) -> date:
    """Return the n-th occurrence (1-based) of *weekday* (0=Mon) in *month*."""
    first = date(year, month, 1)
    first_wday = first.weekday()
    offset = (weekday - first_wday) % 7
    return first + timedelta(days=offset + 7 * (n - 1))


def _last_weekday_of_month(year: int, month: int, weekday: int) -> date:
    """Return the last occurrence of *weekday* (0=Mon) in *month*."""
    if month == 12:
        last_day = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        last_day = date(year, month + 1, 1) - timedelta(days=1)
    offset = (last_day.weekday() - weekday) % 7
    return last_day - timedelta(days=offset)


def _easter_sunday(year: int) -> date:
    """Anonymous Gregorian computus (Meeus/Jones/Butcher)."""
    a = year % 19
    b, c = divmod(year, 100)
    d, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month, day = divmod(h + l - 7 * m + 114, 31)
    return date(year, month, day + 1)


def generate_us_market_holidays(year: int) -> Set[date]:
    """
    Generate the set of US equity/forex-relevant market holidays for *year*.

    Covers: New Year's Day, MLK Day, Presidents' Day, Good Friday,
    Memorial Day, Independence Day (Juneteenth omitted — forex usually
    trades), Labor Day, Thanksgiving, Christmas.

    Observed-date rules: if a holiday falls on Saturday it is observed
    on Friday; if Sunday, on Monday.
    """
    holidays = set()

    def _observed(d: date) -> date:
        if d.weekday() == 5:   # Saturday → Friday
            return d - timedelta(days=1)
        if d.weekday() == 6:   # Sunday → Monday
            return d + timedelta(days=1)
        return d

    # Fixed-date holidays
    holidays.add(_observed(date(year, 1, 1)))     # New Year
    holidays.add(_observed(date(year, 6, 19)))    # Juneteenth (federal since 2021)
    holidays.add(_observed(date(year, 7, 4)))     # Independence Day
    holidays.add(_observed(date(year, 12, 25)))   # Christmas

    # Floating holidays
    holidays.add(_nth_weekday_of_month(year, 1, 0, 3))   # MLK Day (3rd Mon Jan)
    holidays.add(_nth_weekday_of_month(year, 2, 0, 3))   # Presidents' Day (3rd Mon Feb)
    holidays.add(_last_weekday_of_month(year, 5, 0))      # Memorial Day (last Mon May)
    holidays.add(_nth_weekday_of_month(year, 9, 0, 1))   # Labor Day (1st Mon Sep)
    holidays.add(_nth_weekday_of_month(year, 11, 3, 4))  # Thanksgiving (4th Thu Nov)

    # Good Friday (2 days before Easter Sunday)
    holidays.add(_easter_sunday(year) - timedelta(days=2))

    return holidays


def get_us_holidays_for_range(start_year: int, end_year: int) -> Set[date]:
    """Return combined US holiday set for [start_year, end_year]."""
    result: Set[date] = set()
    for y in range(start_year, end_year + 1):
        result |= generate_us_market_holidays(y)
    return result


# ---------------------------------------------------------------------------
# Session-break detection helpers
# ---------------------------------------------------------------------------

def _parse_time(t) -> time:
    """Parse a time value from YAML (str or int) into datetime.time."""
    if isinstance(t, time):
        return t
    s = str(t).strip()
    parts = s.split(":")
    return time(int(parts[0]), int(parts[1]) if len(parts) > 1 else 0)


def get_session_breaks(
    market_hours_for_asset: Dict[str, Any],
    tz_name: str,
) -> List[Tuple[time, time]]:
    """
    Derive intraday break windows from session definitions.

    For a typical weekday (Tuesday is used as canonical) with two sessions
    e.g. [00:00-16:55, 17:05-23:59] the break is 16:55 → 17:05.

    Returns list of (break_start, break_end) in the broker timezone.
    """
    # Use Tuesday as canonical (always a weekday, no edge cases)
    sessions = market_hours_for_asset.get("tuesday", [])
    if not sessions or len(sessions) < 2:
        return []

    breaks = []
    # Sort sessions by open time
    sorted_sessions = sorted(sessions, key=lambda s: _parse_time(s["open"]))
    for i in range(len(sorted_sessions) - 1):
        close_i = _parse_time(sorted_sessions[i]["close"])
        open_next = _parse_time(sorted_sessions[i + 1]["open"])
        if open_next > close_i:
            breaks.append((close_i, open_next))
    return breaks


def is_session_break_gap(
    gap_start_ts: datetime,
    gap_end_ts: datetime,
    session_breaks: List[Tuple[time, time]],
    server_offset_hours: int = 7,
    tolerance_minutes: int = 30,
) -> bool:
    """
    Check whether a gap falls within an expected session break window.

    Session break times (from brokers.yaml) are in ET.  Data timestamps
    are in MT5 server time = ET + server_offset_hours.  We convert the
    break times to server time by adding the offset, then compare the
    gap's time-of-day directly.

    Args:
        gap_start_ts:         Last bar before the gap (server time)
        gap_end_ts:           First bar after the gap (server time)
        session_breaks:       List of (break_start, break_end) in ET
        server_offset_hours:  Hours to add to ET to get server time (7 for MT5)
        tolerance_minutes:    Extra tolerance on each side

    Returns:
        True if the gap matches an expected session break.
    """
    if not session_breaks:
        return False

    def _shift_time(t: time, hours: int, minutes: int = 0) -> Tuple[time, int]:
        """Shift a time by hours+minutes. Returns (new_time, day_overflow)."""
        total_min = t.hour * 60 + t.minute + hours * 60 + minutes
        day_overflow = total_min // (24 * 60)
        total_min = total_min % (24 * 60)
        if total_min < 0:
            total_min += 24 * 60
            day_overflow -= 1
        return time(total_min // 60, total_min % 60), day_overflow

    gap_start_time = gap_start_ts.time() if hasattr(gap_start_ts, 'time') else time(0, 0)
    gap_end_time = gap_end_ts.time() if hasattr(gap_end_ts, 'time') else time(0, 0)

    for break_start_et, break_end_et in session_breaks:
        # Convert break times from ET to server time
        brk_start_srv, _ = _shift_time(break_start_et, server_offset_hours, -tolerance_minutes)
        brk_end_srv, _ = _shift_time(break_end_et, server_offset_hours, tolerance_minutes)

        # Check if gap_end time falls within the server-time break window
        if brk_start_srv <= brk_end_srv:
            # Break does NOT cross midnight
            if brk_start_srv <= gap_end_time <= brk_end_srv:
                return True
        else:
            # Break CROSSES midnight (e.g. 23:58 → 01:01)
            if gap_end_time >= brk_start_srv or gap_end_time <= brk_end_srv:
                return True

        # Also check gap_start time (in case the gap description uses the
        # pre-gap timestamp instead of the post-gap timestamp)
        if brk_start_srv <= brk_end_srv:
            if brk_start_srv <= gap_start_time <= brk_end_srv:
                return True
        else:
            if gap_start_time >= brk_start_srv or gap_start_time <= brk_end_srv:
                return True

    return False


@dataclass
class QualityReport:
    """Summary of data quality checks."""
    symbol: str
    timeframe: str
    total_bars: int
    date_range: Tuple[datetime, datetime]
    issues: List[DataQualityIssue]
    passed: bool
    summary_stats: Dict[str, Any]


class DataQualityChecker:
    """Validates OHLCV data and flags quality issues."""
    
    def __init__(self, _spike_threshold: float = 5.0, _gap_multiplier: float = 2.0, _volume_multiplier: float = 100.0):
        self._spike_threshold = _spike_threshold
        self._gap_multiplier = _gap_multiplier
        self._volume_multiplier = _volume_multiplier
    
    def run_all_checks(self, _data: pd.DataFrame, _symbol: str, _timeframe: str) -> QualityReport:
        """Run all quality checks on dataset."""
        df = _data.copy()
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df.sort_values("timestamp").reset_index(drop=True)
        
        issues = []
        issues.extend(self.check_price_spikes(df))
        issues.extend(self.check_timestamp_gaps(df, _timeframe))
        issues.extend(self.check_volume_anomalies(df))
        issues.extend(self._check_ohlc_consistency(df))
        
        errors = sum(1 for i in issues if i.severity == "error")
        
        return QualityReport(
            symbol=_symbol, timeframe=_timeframe, total_bars=len(df),
            date_range=(df["timestamp"].min().to_pydatetime(), df["timestamp"].max().to_pydatetime()),
            issues=issues, passed=(errors == 0),
            summary_stats={"price_min": df["low"].min(), "price_max": df["high"].max()},
        )
    
    def check_price_spikes(self, _data: pd.DataFrame) -> List[DataQualityIssue]:
        """Find bars with price change > threshold %."""
        df = _data.copy()
        df["pct"] = (df["close"] - df["open"]).abs() / df["open"]
        threshold = self._spike_threshold / 100.0
        
        issues = []
        for _, r in df[df["pct"] > threshold].iterrows():
            issues.append(DataQualityIssue(
                timestamp=r["timestamp"].to_pydatetime(), issue_type="price_spike",
                description=f"{r['pct']*100:.2f}% change", severity="warning", value=r["pct"]*100,
            ))
        return issues
    
    def check_timestamp_gaps(self, _data: pd.DataFrame, _timeframe: str) -> List[DataQualityIssue]:
        """Find timestamp gaps > expected interval."""
        expected_sec = Timeframe.from_string(_timeframe).to_minutes() * 60
        threshold = expected_sec * self._gap_multiplier
        
        df = _data.copy()
        df["diff"] = df["timestamp"].diff().dt.total_seconds()
        
        issues = []
        for idx, r in df[df["diff"] > threshold].iterrows():
            if idx > 0:
                issues.append(DataQualityIssue(
                    timestamp=r["timestamp"].to_pydatetime(), issue_type="timestamp_gap",
                    description=f"Gap of {r['diff']/expected_sec:.1f} bars", severity="warning", value=r["diff"],
                ))
        return issues
    
    def check_volume_anomalies(self, _data: pd.DataFrame) -> List[DataQualityIssue]:
        """Find zero or extremely high volume."""
        avg = _data["volume"].mean()
        high_thresh = avg * self._volume_multiplier
        
        issues = []
        for _, r in _data[_data["volume"] == 0].iterrows():
            issues.append(DataQualityIssue(
                timestamp=r["timestamp"].to_pydatetime(), issue_type="volume_zero",
                description="Zero volume", severity="warning", value=0,
            ))
        
        if avg > 0:
            for _, r in _data[_data["volume"] > high_thresh].iterrows():
                issues.append(DataQualityIssue(
                    timestamp=r["timestamp"].to_pydatetime(), issue_type="volume_high",
                    description=f"Volume {r['volume']/avg:.0f}x average", severity="warning", value=r["volume"],
                ))
        return issues
    
    def _check_ohlc_consistency(self, _data: pd.DataFrame) -> List[DataQualityIssue]:
        """Check OHLC consistency."""
        issues = []
        seen = set()
        
        for _, r in _data[_data["high"] < _data["low"]].iterrows():
            ts = r["timestamp"].to_pydatetime()
            issues.append(DataQualityIssue(ts, "ohlc_invalid", "High < Low", "error"))
            seen.add(ts)
        
        for _, r in _data[(_data["high"] < _data["open"]) | (_data["high"] < _data["close"])].iterrows():
            ts = r["timestamp"].to_pydatetime()
            if ts not in seen:
                issues.append(DataQualityIssue(ts, "ohlc_invalid", "High not >= Open/Close", "error"))
                seen.add(ts)
        
        return issues
    
    def get_summary_report(self, _report: QualityReport) -> str:
        """Generate human-readable summary."""
        lines = [
            f"Quality: {_report.symbol} {_report.timeframe} | {_report.total_bars:,} bars | {'PASSED' if _report.passed else 'FAILED'}",
            f"Issues: {len(_report.issues)}",
        ]
        by_type: Dict[str, int] = {}
        for i in _report.issues:
            by_type[i.issue_type] = by_type.get(i.issue_type, 0) + 1
        for t, c in by_type.items():
            lines.append(f"  {t}: {c}")
        return "\n".join(lines)
    
    def to_dataframe(self, _report: QualityReport) -> pd.DataFrame:
        """
        Convert quality report issues to DataFrame.
        
        Args:
            _report: QualityReport from run_all_checks
        
        Returns:
            DataFrame with columns: timestamp, issue_type, description, severity, value
        """
        if not _report.issues:
            return pd.DataFrame(columns=['timestamp', 'issue_type', 'description', 'severity', 'value'])
        
        rows = []
        for issue in _report.issues:
            rows.append({
                'timestamp': issue.timestamp,
                'issue_type': issue.issue_type,
                'description': issue.description,
                'severity': issue.severity,
                'value': issue.value if hasattr(issue, 'value') else None,
            })
        
        return pd.DataFrame(rows).sort_values('timestamp').reset_index(drop=True)
    
    def get_summary(self, _report: QualityReport) -> Dict[str, Any]:
        """
        Get summary statistics with traffic light scoring.
        
        Args:
            _report: QualityReport from run_all_checks
        
        Returns:
            Dictionary with:
                - total_bars: int
                - date_range: tuple
                - issue_counts: dict by type
                - severity_counts: dict by severity
                - quality_score: float (0-1, higher is better)
                - status: 'green', 'yellow', or 'red'
                - passed: bool
        """
        # Count by type
        by_type: Dict[str, int] = {}
        by_severity: Dict[str, int] = {'error': 0, 'warning': 0, 'info': 0}
        
        for issue in _report.issues:
            by_type[issue.issue_type] = by_type.get(issue.issue_type, 0) + 1
            if issue.severity in by_severity:
                by_severity[issue.severity] += 1
        
        # Calculate quality score (0-1)
        # Penalize: errors heavily, warnings moderately
        error_penalty = by_severity['error'] * 0.1
        warning_penalty = by_severity['warning'] * 0.01
        total_penalty = min(1.0, error_penalty + warning_penalty)
        quality_score = max(0.0, 1.0 - total_penalty)
        
        # Determine status (traffic light)
        if quality_score >= 0.95 and by_severity['error'] == 0:
            status = 'green'
        elif quality_score >= 0.80 and by_severity['error'] == 0:
            status = 'yellow'
        else:
            status = 'red'
        
        return {
            'symbol': _report.symbol,
            'timeframe': _report.timeframe,
            'total_bars': _report.total_bars,
            'date_range': _report.date_range,
            'issue_counts': by_type,
            'severity_counts': by_severity,
            'total_issues': len(_report.issues),
            'quality_score': round(quality_score, 3),
            'status': status,
            'passed': _report.passed,
            'price_min': _report.summary_stats.get('price_min'),
            'price_max': _report.summary_stats.get('price_max'),
        }
    
    def plot_report(
        self,
        _data: pd.DataFrame,
        _report: QualityReport,
        _output_path: Optional[str] = None
    ) -> None:
        """
        Create comprehensive quality report visualization.
        
        Shows:
        - Price chart with issues highlighted
        - Issue distribution by type (bar chart)
        - Traffic light status indicator
        - Summary statistics table
        
        Args:
            _data: OHLCV DataFrame
            _report: QualityReport from run_all_checks
            _output_path: Optional path to save figure
        """
        try:
            import matplotlib.pyplot as plt
            from matplotlib.patches import Circle
            import matplotlib.gridspec as gridspec
        except ImportError:
            print("matplotlib not installed")
            return
        
        summary = self.get_summary(_report)
        df = _data.copy()
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        
        # Create figure with grid layout
        fig = plt.figure(figsize=(16, 10))
        gs = gridspec.GridSpec(3, 3, figure=fig, height_ratios=[2, 1, 1])
        
        # Main price chart (top, spans 2 columns)
        ax_price = fig.add_subplot(gs[0, :2])
        ax_price.plot(df["timestamp"], df["close"], "b-", lw=0.5, label="Close")
        
        # Highlight issues on price chart
        colors = {'price_spike': 'red', 'timestamp_gap': 'orange', 'volume_zero': 'purple', 'volume_high': 'yellow', 'ohlc_invalid': 'black'}
        for issue_type, color in colors.items():
            issues = [i for i in _report.issues if i.issue_type == issue_type]
            if issues:
                ts = [i.timestamp for i in issues]
                mask = df["timestamp"].isin(ts)
                if mask.any():
                    ax_price.scatter(df.loc[mask, "timestamp"], df.loc[mask, "close"], c=color, s=30, label=issue_type, zorder=5)
        
        ax_price.set_title(f"Data Quality: {_report.symbol} {_report.timeframe}", fontsize=14)
        ax_price.set_xlabel("Time")
        ax_price.set_ylabel("Price")
        ax_price.legend(loc='upper left', fontsize=8)
        ax_price.grid(True, alpha=0.3)
        
        # Traffic light indicator (top right)
        ax_light = fig.add_subplot(gs[0, 2])
        ax_light.set_xlim(-1, 1)
        ax_light.set_ylim(-1, 1)
        ax_light.set_aspect('equal')
        ax_light.axis('off')
        
        # Draw traffic light
        light_colors = {'green': '#2ecc71', 'yellow': '#f1c40f', 'red': '#e74c3c'}
        active_color = light_colors.get(summary['status'], '#95a5a6')
        circle = Circle((0, 0), 0.5, color=active_color, ec='black', lw=2)
        ax_light.add_patch(circle)
        ax_light.text(0, 0, f"{summary['quality_score']:.1%}", ha='center', va='center', fontsize=24, fontweight='bold', color='white')
        ax_light.text(0, -0.8, summary['status'].upper(), ha='center', va='center', fontsize=16, fontweight='bold')
        
        # Issue distribution bar chart (middle left)
        ax_bars = fig.add_subplot(gs[1, :2])
        if summary['issue_counts']:
            types = list(summary['issue_counts'].keys())
            counts = list(summary['issue_counts'].values())
            bar_colors = [colors.get(t, 'gray') for t in types]
            ax_bars.barh(types, counts, color=bar_colors)
            ax_bars.set_xlabel("Count")
            ax_bars.set_title("Issues by Type")
        else:
            ax_bars.text(0.5, 0.5, "No Issues Found", ha='center', va='center', fontsize=14)
            ax_bars.axis('off')
        
        # Summary table (middle right)
        ax_table = fig.add_subplot(gs[1, 2])
        ax_table.axis('off')
        
        table_data = [
            ["Total Bars", f"{summary['total_bars']:,}"],
            ["Date Range", f"{summary['date_range'][0].strftime('%Y-%m-%d')} to {summary['date_range'][1].strftime('%Y-%m-%d')}"],
            ["Total Issues", str(summary['total_issues'])],
            ["Errors", str(summary['severity_counts']['error'])],
            ["Warnings", str(summary['severity_counts']['warning'])],
            ["Price Range", f"{summary['price_min']:.5f} - {summary['price_max']:.5f}"],
            ["Status", "PASSED" if summary['passed'] else "FAILED"],
        ]
        
        table = ax_table.table(cellText=table_data, colLabels=["Metric", "Value"], loc='center', cellLoc='left')
        table.auto_set_font_size(False)
        table.set_fontsize(10)
        table.scale(1.2, 1.5)
        
        # Severity pie chart (bottom)
        ax_pie = fig.add_subplot(gs[2, 1])
        severity_data = summary['severity_counts']
        if sum(severity_data.values()) > 0:
            labels = [k for k, v in severity_data.items() if v > 0]
            sizes = [v for v in severity_data.values() if v > 0]
            pie_colors = {'error': '#e74c3c', 'warning': '#f1c40f', 'info': '#3498db'}
            ax_pie.pie(sizes, labels=labels, autopct='%1.0f%%', colors=[pie_colors.get(l, 'gray') for l in labels])
            ax_pie.set_title("Issues by Severity")
        else:
            ax_pie.text(0.5, 0.5, "No Issues", ha='center', va='center', fontsize=12)
            ax_pie.axis('off')
        
        plt.tight_layout()
        
        if _output_path:
            plt.savefig(_output_path, dpi=150, bbox_inches='tight')
        else:
            plt.show()
        plt.close()
    
    def visualize_issues(self, _data: pd.DataFrame, _report: QualityReport, _output_path: Optional[str] = None) -> None:
        """Create quality visualization."""
        try:
            import matplotlib.pyplot as plt
        except ImportError:
            print("matplotlib not installed")
            return
        
        fig, ax = plt.subplots(figsize=(14, 6))
        df = _data.copy()
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        
        ax.plot(df["timestamp"], df["close"], "b-", lw=0.5)
        spikes = [i for i in _report.issues if i.issue_type == "price_spike"]
        if spikes:
            ts = [i.timestamp for i in spikes]
            prices = df[df["timestamp"].isin(ts)]["close"]
            ax.scatter(ts, prices, c="red", s=50, zorder=5)
        
        ax.set_title(f"{_report.symbol} {_report.timeframe} Quality")
        plt.tight_layout()
        if _output_path:
            plt.savefig(_output_path, dpi=150)
        else:
            plt.show()
        plt.close()

    def validate_and_repair(
        self,
        _data: pd.DataFrame,
        _symbol: str,
        _timeframe: str,
        _remove_duplicates: bool = True,
        _sort: bool = True,
        _log_gaps: bool = True,
        _broker_name: Optional[str] = None,
        _asset_class: Optional[str] = None,
        _market_hours: Optional[Dict[str, Any]] = None,
        _holiday_dates: Optional[Set[date]] = None,
    ) -> Tuple[pd.DataFrame, QualityReport]:
        """
        Validate and repair OHLCV data on load.
        
        Called automatically when loading data from broker or parquet to ensure
        data quality before backtesting. This prevents false price discontinuities
        in the dynamic chart caused by duplicate timestamps, unsorted data, or
        OHLC inconsistencies.
        
        Gap suppression (Phase 5.11):
            When _market_hours and _asset_class are provided, expected session
            break gaps (e.g. the nightly 10-min forex break at 17:00 ET) are
            suppressed from the console report.  When _holiday_dates is provided
            (or auto-generated from the data range), gaps on US holidays are also
            suppressed.  Only genuinely unexpected intraday gaps are reported.
        
        Steps:
            1. Sort by timestamp
            2. Remove exact duplicate timestamps (keep last)
            3. Check OHLC consistency (high >= low, high >= open/close)
            4. Flag timestamp gaps (weekends, session breaks, holidays excluded)
            5. Flag price spikes
            6. Return cleaned DataFrame + QualityReport
        
        Args:
            _data: OHLCV DataFrame with 'timestamp' column or DatetimeIndex
            _symbol: Instrument symbol
            _timeframe: Timeframe string (e.g. 'M15')
            _remove_duplicates: Remove duplicate timestamps (default True)
            _sort: Sort by timestamp (default True)
            _log_gaps: Print gap warnings to console (default True)
            _broker_name: Broker name (for session-break detection context)
            _asset_class: Instrument asset class ('forex', 'commodity', 'index', etc.)
            _market_hours: Broker market_hours config dict (from brokers.yaml).
                           Keys are asset class labels (forex, metals, indices).
            _holiday_dates: Set of date objects for known market holidays.
                            If None and data is available, US holidays are
                            auto-generated for the data's year range.
        
        Returns:
            Tuple of (cleaned DataFrame, QualityReport)
        """
        df = _data.copy()
        
        # Ensure timestamp column exists
        if isinstance(df.index, pd.DatetimeIndex):
            if 'timestamp' not in df.columns:
                df['timestamp'] = df.index
            df = df.reset_index(drop=True)
        
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        original_len = len(df)
        
        # Step 1: Sort
        if _sort:
            df = df.sort_values('timestamp').reset_index(drop=True)
        
        # Step 2: Remove duplicate timestamps
        if _remove_duplicates:
            before = len(df)
            df = df.drop_duplicates(subset='timestamp', keep='last').reset_index(drop=True)
            removed = before - len(df)
            if removed > 0:
                print(f"  DataQuality [{_symbol} {_timeframe}]: Removed {removed} duplicate timestamps")
        
        # Step 3: Run quality checks
        report = self.run_all_checks(df, _symbol, _timeframe)
        
        # Step 4: Log significant gaps (with session-break + holiday filtering)
        if _log_gaps:
            gap_issues = [i for i in report.issues if i.issue_type == 'timestamp_gap']
            if gap_issues:
                # --- Resolve session breaks from market hours ---
                session_breaks: List[Tuple[time, time]] = []
                server_offset_hours = 7  # default for MT5 brokers

                if _market_hours and _asset_class:
                    mh_key = _ASSET_CLASS_TO_MARKET_HOURS_KEY.get(
                        _asset_class.lower(), None
                    )
                    server_offset_hours = _market_hours.get(
                        "server_offset_hours", server_offset_hours
                    )
                    if mh_key and mh_key in _market_hours:
                        session_breaks = get_session_breaks(
                            _market_hours[mh_key],
                            _market_hours.get("timezone", "America/New_York"),
                        )

                # --- Resolve holiday dates ---
                holidays = _holiday_dates
                if holidays is None and len(df) > 0:
                    start_year = df['timestamp'].min().year
                    end_year = df['timestamp'].max().year
                    holidays = get_us_holidays_for_range(start_year, end_year)

                # --- Classify each gap ---
                unexpected_gaps = []
                suppressed_session = 0
                suppressed_holiday = 0
                suppressed_weekend = 0

                # Build a sorted timestamp array for gap-start lookup
                timestamps = df['timestamp'].values

                for issue in gap_issues:
                    ts = issue.timestamp  # gap end (first bar after gap)

                    # (a) Weekend gap: resuming on Monday with <= 3 day gap
                    if hasattr(ts, 'weekday'):
                        weekday = ts.weekday()
                        if weekday == 0 and issue.value <= 3.5 * 86400:
                            suppressed_weekend += 1
                            continue
                        # Also catch Sunday-open gaps (some brokers open Sun 17:00 ET)
                        if weekday == 6 and issue.value <= 2 * 86400:
                            suppressed_weekend += 1
                            continue

                    # Find the gap-start timestamp (bar before the gap)
                    gap_end_ts = pd.Timestamp(ts)
                    # Locate position of gap_end in the data
                    pos = df.index[df['timestamp'] == gap_end_ts]
                    if len(pos) > 0 and pos[0] > 0:
                        gap_start_ts = df['timestamp'].iloc[pos[0] - 1]
                    else:
                        gap_start_ts = gap_end_ts - pd.Timedelta(seconds=issue.value)

                    # (b) Holiday gap — checked BEFORE session breaks.
                    # Server time offset: a US holiday (e.g. July 4 in ET) maps to
                    # the same date or next date in server time.  We check the gap_end
                    # date and the day before (to catch the offset).
                    if holidays:
                        gap_start_date = (
                            gap_start_ts.date()
                            if hasattr(gap_start_ts, 'date') and callable(gap_start_ts.date)
                            else pd.Timestamp(gap_start_ts).date()
                        )
                        gap_end_date = (
                            gap_end_ts.date()
                            if hasattr(gap_end_ts, 'date') and callable(gap_end_ts.date)
                            else pd.Timestamp(gap_end_ts).date()
                        )
                        # Check all dates in gap range + day before (server TZ offset)
                        is_holiday = False
                        day_cursor = gap_start_date - timedelta(days=1)
                        while day_cursor <= gap_end_date:
                            if day_cursor in holidays:
                                is_holiday = True
                                break
                            day_cursor += timedelta(days=1)
                        if is_holiday:
                            suppressed_holiday += 1
                            continue

                    # (c) Session break gap
                    if session_breaks:
                        if is_session_break_gap(
                            gap_start_ts.to_pydatetime()
                            if hasattr(gap_start_ts, 'to_pydatetime')
                            else gap_start_ts,
                            gap_end_ts.to_pydatetime()
                            if hasattr(gap_end_ts, 'to_pydatetime')
                            else gap_end_ts,
                            session_breaks,
                            server_offset_hours=server_offset_hours,
                            tolerance_minutes=30,
                        ):
                            suppressed_session += 1
                            continue

                    # (d) Genuinely unexpected gap
                    unexpected_gaps.append(issue)

                # --- Print summary ---
                total_gaps = len(gap_issues)
                total_suppressed = suppressed_weekend + suppressed_session + suppressed_holiday
                if unexpected_gaps:
                    print(
                        f"  DataQuality [{_symbol} {_timeframe}]: "
                        f"{len(unexpected_gaps)} unexpected gaps "
                        f"(of {total_gaps} total; "
                        f"{suppressed_session} session breaks, "
                        f"{suppressed_holiday} holidays, "
                        f"{suppressed_weekend} weekends suppressed):"
                    )
                    for gap in unexpected_gaps:
                        print(f"    Gap at {gap.timestamp}: {gap.description}")
                elif total_gaps > 0:
                    print(
                        f"  DataQuality [{_symbol} {_timeframe}]: "
                        f"{total_gaps} gaps all expected "
                        f"({suppressed_session} session breaks, "
                        f"{suppressed_holiday} holidays, "
                        f"{suppressed_weekend} weekends)"
                    )
        
        # Step 5: Flag OHLC errors
        ohlc_errors = [i for i in report.issues if i.issue_type == 'ohlc_invalid']
        if ohlc_errors:
            print(f"  DataQuality [{_symbol} {_timeframe}]: {len(ohlc_errors)} OHLC consistency errors")
        
        if len(df) < original_len:
            print(f"  DataQuality [{_symbol} {_timeframe}]: {original_len} -> {len(df)} bars after cleanup")
        
        return df, report

    def save_detailed_report(
        self, _report: 'QualityReport', _output_path: str,
        _symbol: str = '', _timeframe: str = '',
    ) -> str:
        """
        Save a detailed data quality report to a text file.

        Includes: summary, all issues by type (spikes, gaps, volume, OHLC),
        timestamps, descriptions, and severity.

        Args:
            _report: QualityReport from run_all_checks
            _output_path: File path for the report (e.g. './_exports/quality_report.txt')
            _symbol: Symbol name for the header
            _timeframe: Timeframe for the header

        Returns:
            Path to the saved report.
        """
        import os
        os.makedirs(os.path.dirname(_output_path) if os.path.dirname(_output_path) else '.', exist_ok=True)

        lines = []
        lines.append("=" * 70)
        lines.append(f"  DATA QUALITY REPORT — {_symbol} {_timeframe}")
        lines.append("=" * 70)
        lines.append(f"  Total bars:   {_report.total_bars:,}")
        lines.append(f"  Date range:   {_report.date_range[0]} → {_report.date_range[1]}")
        lines.append(f"  Status:       {'PASSED' if _report.passed else 'FAILED'}")
        lines.append(f"  Total issues: {len(_report.issues)}")
        lines.append("")

        # Group issues by type
        by_type: Dict[str, List] = {}
        for issue in _report.issues:
            by_type.setdefault(issue.issue_type, []).append(issue)

        for issue_type, issues in sorted(by_type.items()):
            lines.append(f"--- {issue_type.upper()} ({len(issues)} issues) ---")
            for iss in issues[:50]:  # cap at 50 per type
                val_str = f" [{iss.value:.2f}]" if hasattr(iss, 'value') and iss.value is not None else ""
                lines.append(f"  {iss.severity:7s} | {iss.timestamp} | {iss.description}{val_str}")
            if len(issues) > 50:
                lines.append(f"  ... and {len(issues) - 50} more")
            lines.append("")

        # Price stats
        if _report.summary_stats:
            lines.append("--- PRICE STATS ---")
            for k, v in _report.summary_stats.items():
                lines.append(f"  {k}: {v}")
            lines.append("")

        report_text = "\n".join(lines)
        with open(_output_path, 'w', encoding='utf-8') as f:
            f.write(report_text)
        print(f"  DataQuality: detailed report saved to {_output_path}")
        return _output_path
