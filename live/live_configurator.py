"""
Live Configurator Module
=========================
YAML-driven configuration loader for live trading parameters.

Sections (P2.1 + P2.2 + P3.1 + P3.2 + P7.2):
    - engine: history_window, poll_interval_seconds, timeframe
    - circuit_breakers: max_daily_dd_pct, max_total_dd_pct, max_position_lots
    - data_validation: max_gap_bars, forward_fill_gaps
    - market_hours: asset_class, enabled
    - heartbeat: enabled, check_interval_seconds, stale_data_threshold_seconds
    - slippage: enabled
    - spread_filter: enabled, max_spread_pips (P2.1)
    - audit_trail: enabled, audit_dir, flush_interval_records (P2.1)
    - partial_fills: enabled, max_retries (P2.1)
    - kill_switch: enabled, sentinel_path, poll_interval_seconds (P2.2)
    - idempotency: enabled (P2.2)
    - execution_monitor: enabled, output_dir (P2.2)
    - paper_trade: enabled, initial_balance, slippage_pips, commission_per_lot (P3.1)
    - shadow_mode: mode, position_size_tolerance, history_window (P3.2)
    - logging: level, log_file, print_timing
    - calendar_news: finnhub_api_key, calendar_db_dir, backfill_start (P7.2, optional)

Classes:
    LiveConfigurator:
        - load, get_engine_settings, get_circuit_breakers,
          get_data_validation, get_market_hours, get_heartbeat,
          get_slippage, get_spread_filter, get_audit_trail,
          get_partial_fills, get_kill_switch, get_idempotency,
          get_execution_monitor, get_paper_trade, get_shadow_mode,
          get_logging, get_calendar_news, reload

Usage:
    config = LiveConfigurator(_path="live/live_params.yaml")
    engine_settings = config.get_engine_settings()
    calendar_news = config.get_calendar_news()
    finnhub_key = calendar_news['finnhub_api_key']  # '' if not configured
"""

from pathlib import Path
from typing import Any, Dict, Optional

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.exceptions import ConfigurationError

try:
    import yaml
except ImportError:
    yaml = None


class LiveConfigurator:
    """
    YAML-driven configuration loader for live trading parameters.

    All parameters are explicit — no hidden defaults. If a required key
    is missing from the YAML file, a ConfigurationError is raised.
    """

    # Required top-level keys in the YAML
    _REQUIRED_SECTIONS = [
        'engine', 'circuit_breakers', 'data_validation',
        'market_hours', 'heartbeat', 'slippage',
        'spread_filter', 'audit_trail', 'partial_fills',
        'kill_switch', 'idempotency', 'execution_monitor',
        'paper_trade', 'shadow_mode',
        'logging',
    ]

    def __init__(self, _path: str = "live/live_params.yaml"):
        """
        Initialize LiveConfigurator.

        Args:
            _path: Path to live_params.yaml file.
        """
        self._path = Path(_path)
        self._config: Dict[str, Any] = {}
        self._load()

    def _load(self) -> None:
        """Load configuration from YAML file."""
        if yaml is None:
            raise ConfigurationError("PyYAML not installed", {"path": str(self._path)})

        if not self._path.exists():
            raise ConfigurationError(
                f"Live params file not found: {self._path}",
                {"path": str(self._path)},
            )

        with open(self._path, 'r') as f:
            raw = yaml.safe_load(f)

        if raw is None or 'live' not in raw:
            raise ConfigurationError(
                "live_params.yaml must have a top-level 'live' key",
                {"path": str(self._path)},
            )

        self._config = raw['live']

        # Validate required sections
        for section in self._REQUIRED_SECTIONS:
            if section not in self._config:
                raise ConfigurationError(
                    f"Missing required section '{section}' in live_params.yaml",
                    {"path": str(self._path), "missing": section},
                )

    def get_engine_settings(self) -> Dict[str, Any]:
        """
        Get engine settings.

        Returns:
            Dict with keys: history_window, poll_interval_seconds, timeframe
        """
        engine = self._config['engine']
        required_keys = ['history_window', 'poll_interval_seconds', 'timeframe']
        for key in required_keys:
            if key not in engine:
                raise ConfigurationError(
                    f"Missing required key 'engine.{key}' in live_params.yaml",
                    {"path": str(self._path)},
                )
        return dict(engine)

    def get_circuit_breakers(self) -> Dict[str, Any]:
        """
        Get circuit breaker settings (account-level risk limits).

        Returns:
            Dict with keys: max_daily_dd_pct, max_total_dd_pct, max_position_lots
        """
        cb = self._config['circuit_breakers']
        required_keys = ['max_daily_dd_pct', 'max_total_dd_pct', 'max_position_lots']
        for key in required_keys:
            if key not in cb:
                raise ConfigurationError(
                    f"Missing required key 'circuit_breakers.{key}' in live_params.yaml",
                    {"path": str(self._path)},
                )
        return dict(cb)

    def get_data_validation(self) -> Dict[str, Any]:
        """
        Get data validation settings.

        Returns:
            Dict with keys: max_gap_bars, forward_fill_gaps
        """
        dv = self._config['data_validation']
        required_keys = ['max_gap_bars', 'forward_fill_gaps']
        for key in required_keys:
            if key not in dv:
                raise ConfigurationError(
                    f"Missing required key 'data_validation.{key}' in live_params.yaml",
                    {"path": str(self._path)},
                )
        return dict(dv)

    def get_market_hours(self) -> Dict[str, Any]:
        """
        Get market hours settings.

        Returns:
            Dict with keys: asset_class, enabled
        """
        mh = self._config['market_hours']
        required_keys = ['asset_class', 'enabled']
        for key in required_keys:
            if key not in mh:
                raise ConfigurationError(
                    f"Missing required key 'market_hours.{key}' in live_params.yaml",
                    {"path": str(self._path)},
                )
        return dict(mh)

    def get_heartbeat(self) -> Dict[str, Any]:
        """
        Get heartbeat / connectivity monitoring settings.

        Returns:
            Dict with keys: enabled, check_interval_seconds, stale_data_threshold_seconds
        """
        hb = self._config['heartbeat']
        required_keys = ['enabled', 'check_interval_seconds', 'stale_data_threshold_seconds']
        for key in required_keys:
            if key not in hb:
                raise ConfigurationError(
                    f"Missing required key 'heartbeat.{key}' in live_params.yaml",
                    {"path": str(self._path)},
                )
        return dict(hb)

    def get_slippage(self) -> Dict[str, Any]:
        """
        Get slippage tracking settings.

        Returns:
            Dict with keys: enabled
        """
        sl = self._config['slippage']
        required_keys = ['enabled']
        for key in required_keys:
            if key not in sl:
                raise ConfigurationError(
                    f"Missing required key 'slippage.{key}' in live_params.yaml",
                    {"path": str(self._path)},
                )
        return dict(sl)

    def get_spread_filter(self) -> Dict[str, Any]:
        """
        Get spread filter settings (P2.1).

        Returns:
            Dict with keys: enabled, max_spread_pips
        """
        sf = self._config['spread_filter']
        required_keys = ['enabled', 'max_spread_pips']
        for key in required_keys:
            if key not in sf:
                raise ConfigurationError(
                    f"Missing required key 'spread_filter.{key}' in live_params.yaml",
                    {"path": str(self._path)},
                )
        return dict(sf)

    def get_audit_trail(self) -> Dict[str, Any]:
        """
        Get trade journal / audit trail settings (P2.1).

        Returns:
            Dict with keys: enabled, audit_dir, flush_interval_records
        """
        tj = self._config['audit_trail']
        required_keys = ['enabled', 'audit_dir', 'flush_interval_records']
        for key in required_keys:
            if key not in tj:
                raise ConfigurationError(
                    f"Missing required key 'audit_trail.{key}' in live_params.yaml",
                    {"path": str(self._path)},
                )
        return dict(tj)

    def get_partial_fills(self) -> Dict[str, Any]:
        """
        Get partial fill handling settings (P2.1).

        Returns:
            Dict with keys: enabled, max_retries
        """
        pf = self._config['partial_fills']
        required_keys = ['enabled', 'max_retries']
        for key in required_keys:
            if key not in pf:
                raise ConfigurationError(
                    f"Missing required key 'partial_fills.{key}' in live_params.yaml",
                    {"path": str(self._path)},
                )
        return dict(pf)

    def get_kill_switch(self) -> Dict[str, Any]:
        """
        Get kill switch / emergency flatten settings (P2.2).

        Returns:
            Dict with keys: enabled, sentinel_path, poll_interval_seconds
        """
        ks = self._config['kill_switch']
        required_keys = ['enabled', 'sentinel_path', 'poll_interval_seconds']
        for key in required_keys:
            if key not in ks:
                raise ConfigurationError(
                    f"Missing required key 'kill_switch.{key}' in live_params.yaml",
                    {"path": str(self._path)},
                )
        return dict(ks)

    def get_idempotency(self) -> Dict[str, Any]:
        """
        Get idempotent order submission settings (P2.2).

        Returns:
            Dict with keys: enabled
        """
        idem = self._config['idempotency']
        required_keys = ['enabled']
        for key in required_keys:
            if key not in idem:
                raise ConfigurationError(
                    f"Missing required key 'idempotency.{key}' in live_params.yaml",
                    {"path": str(self._path)},
                )
        return dict(idem)

    def get_execution_monitor(self) -> Dict[str, Any]:
        """
        Get execution quality monitoring settings (P2.2).

        Returns:
            Dict with keys: enabled, output_dir
        """
        em = self._config['execution_monitor']
        required_keys = ['enabled', 'output_dir']
        for key in required_keys:
            if key not in em:
                raise ConfigurationError(
                    f"Missing required key 'execution_monitor.{key}' in live_params.yaml",
                    {"path": str(self._path)},
                )
        return dict(em)

    def get_paper_trade(self) -> Dict[str, Any]:
        """
        Get paper trading mode settings (P3.1).

        Returns:
            Dict with keys: enabled, initial_balance, slippage_pips,
            commission_per_lot
        """
        pt = self._config['paper_trade']
        required_keys = [
            'enabled', 'initial_balance', 'slippage_pips',
            'commission_per_lot',
        ]
        for key in required_keys:
            if key not in pt:
                raise ConfigurationError(
                    f"Missing required key 'paper_trade.{key}' in live_params.yaml",
                    {"path": str(self._path)},
                )
        return dict(pt)

    def get_shadow_mode(self) -> Dict[str, Any]:
        """
        Get shadow mode / live validation settings (P3.2).

        Returns:
            Dict with keys: mode, position_size_tolerance, history_window
        """
        sm = self._config['shadow_mode']
        required_keys = ['mode', 'position_size_tolerance', 'history_window']
        for key in required_keys:
            if key not in sm:
                raise ConfigurationError(
                    f"Missing required key 'shadow_mode.{key}' in live_params.yaml",
                    {"path": str(self._path)},
                )
        return dict(sm)

    def get_logging(self) -> Dict[str, Any]:
        """
        Get logging settings.

        Returns:
            Dict with keys: level, log_file, print_timing
        """
        log = self._config['logging']
        required_keys = ['level', 'log_file', 'print_timing']
        for key in required_keys:
            if key not in log:
                raise ConfigurationError(
                    f"Missing required key 'logging.{key}' in live_params.yaml",
                    {"path": str(self._path)},
                )
        return dict(log)

    def get_calendar_news(self) -> Dict[str, Any]:
        """
        Get Finnhub calendar / news filter settings.

        Optional section — returns safe defaults if missing from YAML.
        The API key placeholder 'YOUR_FINNHUB_API_KEY_HERE' is treated
        as absent (returns empty string).

        Returns:
            Dict with keys: finnhub_api_key, calendar_db_dir, backfill_start
        """
        defaults = {
            'finnhub_api_key': '',
            'calendar_db_dir': 'data/calendar_news_db',
            'backfill_start': '2023-01-01',
        }
        cn = self._config.get('calendar_news', {})
        if not cn:
            return defaults
        result = {**defaults, **cn}
        # Treat placeholder as empty
        if result.get('finnhub_api_key', '') == 'YOUR_FINNHUB_API_KEY_HERE':
            result['finnhub_api_key'] = ''
        return result

    def reload(self) -> None:
        """Reload configuration from YAML file."""
        self._load()

    @property
    def config(self) -> Dict[str, Any]:
        """Get full config dict."""
        return dict(self._config)
