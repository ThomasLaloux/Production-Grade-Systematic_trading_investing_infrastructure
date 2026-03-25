"""
Export Manager Module
=====================
Centralized export management with version-based naming.

All exported files use a version prefix (e.g., v00001) instead of date-based naming.
Files are organized into a single _exports folder with subfolders for charts and WF details.

Folder structure:
    _exports/
        simulation_versions.xlsx        # persistent versioning log (never deleted)
        v00001_SMACross_EURUSDp_M15_adv_regime_uptrend.xlsx  # transposed results
        _charts/
            v00001_rank_001_param_0012.jpg
        _dynamic_charts/
            ...
        _wf_details/
            v00001_wf_windows.xlsx      # windows as columns, metrics as rows

Classes:
    ExportManager
        - version_prefix           (property) -> "v00001"
        - update_simulation_versions(...)     -> append row to simulation_versions.xlsx
        - export_simulation_results(result)   -> transposed xlsx (metrics as rows)
        - export_wf_window_details(result)    -> windows as columns xlsx
        - get_chart_path(rank, param_id)      -> path in _charts/
        - get_dynamic_chart_path(rank, param_id) -> path in _dynamic_charts/
        - get_results_path(suffix)            -> path in _exports/
        - get_wf_details_path(suffix)         -> path in _wf_details/
"""

import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import pandas as pd
import numpy as np


class ExportManager:
    """
    Centralized export manager with version-based file naming.
    
    All files start with a version prefix (e.g., v00001) for traceability.
    The simulation_versions.xlsx file is persistent and appended across runs.
    """
    
    def __init__(
        self,
        _version: int,
        _export_dir: str = "./_exports",
    ):
        self._version = _version
        self._export_dir = Path(_export_dir)
        self._charts_dir = self._export_dir / "_charts"
        self._dynamic_charts_dir = self._export_dir / "_dynamic_charts"
        self._wf_details_dir = self._export_dir / "_wf_details"
        self._trades_dir = self._export_dir / "_trades"
        
        # create directories
        self._export_dir.mkdir(parents=True, exist_ok=True)
        self._charts_dir.mkdir(parents=True, exist_ok=True)
        self._dynamic_charts_dir.mkdir(parents=True, exist_ok=True)
        self._wf_details_dir.mkdir(parents=True, exist_ok=True)
        self._trades_dir.mkdir(parents=True, exist_ok=True)
    
    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------
    
    @property
    def version_prefix(self) -> str:
        """Return version string like 'v00001'."""
        return f"v{self._version:05d}"
    
    @property
    def export_dir(self) -> Path:
        return self._export_dir
    
    @property
    def charts_dir(self) -> Path:
        """Static charts directory (_exports/_charts/)."""
        return self._charts_dir

    @property
    def dynamic_charts_dir(self) -> Path:
        """Dynamic (HTML) charts directory (_exports/_dynamic_charts/)."""
        return self._dynamic_charts_dir
    
    @property
    def wf_details_dir(self) -> Path:
        return self._wf_details_dir
    
    @property
    def trades_dir(self) -> Path:
        return self._trades_dir
    
    # ------------------------------------------------------------------
    # Path helpers
    # ------------------------------------------------------------------
    
    def get_results_path(self, _suffix: str, _ext: str = ".xlsx") -> str:
        """Build path: _exports/v00001_<suffix>.xlsx"""
        return str(self._export_dir / f"{self.version_prefix}_{_suffix}{_ext}")
    
    def get_trades_path(self, _suffix: str, _ext: str = ".csv") -> str:
        """Build path: _exports/_trades/v00001_<suffix>.csv"""
        return str(self._trades_dir / f"{self.version_prefix}_{_suffix}{_ext}")
    
    def get_chart_path(self, _rank: int, _param_id: int, _extra: str = "") -> str:
        """Build path: _exports/_charts/v00001_rank_001_param_0012.jpg"""
        extra = f"_{_extra}" if _extra else ""
        filename = f"{self.version_prefix}_rank_{_rank:03d}_param_{_param_id:04d}{extra}.jpg"
        return str(self._charts_dir / filename)
    
    def get_dynamic_chart_path(self, _rank: int, _param_id: int, _extra: str = "") -> str:
        """Build path: _exports/_dynamic_charts/v00001_rank_001_param_0012.jpg"""
        extra = f"_{_extra}" if _extra else ""
        filename = f"{self.version_prefix}_rank_{_rank:03d}_param_{_param_id:04d}{extra}.jpg"
        return str(self._dynamic_charts_dir / filename)
    
    def get_chart_path_custom(self, _suffix: str, _ext: str = ".jpg") -> str:
        """Build path: _exports/_charts/v00001_<suffix>.jpg"""
        return str(self._charts_dir / f"{self.version_prefix}_{_suffix}{_ext}")
    
    def get_dynamic_chart_path_custom(self, _suffix: str, _ext: str = ".jpg") -> str:
        """Build path: _exports/_dynamic_charts/v00001_<suffix>.jpg"""
        return str(self._dynamic_charts_dir / f"{self.version_prefix}_{_suffix}{_ext}")
    
    def get_wf_details_path(self, _suffix: str = "wf_windows", _ext: str = ".xlsx") -> str:
        """Build path: _exports/_wf_details/v00001_wf_windows.xlsx"""
        return str(self._wf_details_dir / f"{self.version_prefix}_{_suffix}{_ext}")
    
    def get_params_path(self, _suffix: str, _ext: str = ".yaml") -> str:
        """Build path: _exports/v00001_<suffix>.yaml"""
        return str(self._export_dir / f"{self.version_prefix}_{_suffix}{_ext}")
    
    # ------------------------------------------------------------------
    # Save / load best parameters
    # ------------------------------------------------------------------
    
    def save_best_params(
        self,
        _params: dict,
        _strategy_name: str,
        _suffix: str = "best_params",
        _rank: int = 1,
        _param_id: int = None,
        _instrument: str = "",
        _timeframe: str = "",
        _notes: str = "",
    ) -> str:
        """
        Save selected best parameters to YAML for later reload.
        
        Args:
            _params: Parameter dictionary (e.g. {'_fast_period': 50, '_slow_period': 100, ...})
            _strategy_name: Strategy name (e.g. 'SMACross')
            _suffix: File suffix (default: 'best_params')
            _rank: Ranking position that was selected
            _param_id: Param combination ID from optimization (if available)
            _instrument: Instrument/symbol
            _timeframe: Timeframe
            _notes: Optional notes
        
        Returns:
            Path to saved YAML file
        """
        import yaml
        from datetime import datetime
        
        output = {
            'version': self.version_prefix,
            'strategy': _strategy_name,
            'instrument': _instrument,
            'timeframe': _timeframe,
            'selected_rank': _rank,
            'param_id': _param_id,
            'parameters': _params,
            'notes': _notes,
            'saved_at': datetime.now().isoformat(),
        }
        
        output_path = self.get_params_path(_suffix)
        with open(output_path, 'w') as f:
            yaml.dump(output, f, default_flow_style=False, sort_keys=False)
        
        print(f"  Best params saved to: {output_path}")
        return output_path
    
    def load_best_params(self, _suffix: str = "best_params") -> dict:
        """
        Load saved best parameters from YAML.
        
        Args:
            _suffix: File suffix used when saving
        
        Returns:
            Parameter dictionary ready to pass to strategy constructor
        
        Raises:
            FileNotFoundError: If the file does not exist
        """
        import yaml
        
        file_path = Path(self.get_params_path(_suffix))
        if not file_path.exists():
            raise FileNotFoundError(f"Params file not found: {file_path}")
        
        with open(file_path, 'r') as f:
            data = yaml.safe_load(f)
        
        params = data.get('parameters', {})
        print(f"  Loaded params from: {file_path}")
        print(f"    Strategy:  {data.get('strategy', '?')}")
        print(f"    Rank:      {data.get('selected_rank', '?')}")
        print(f"    Param ID:  {data.get('param_id', '?')}")
        print(f"    Params:    {params}")
        return params
    
    # ------------------------------------------------------------------
    # Simulation versions log
    # ------------------------------------------------------------------
    
    def update_simulation_versions(
        self,
        _instrument: str,
        _timeframe: str,
        _strategy_name: str,
        _optim_type: str,
        _regime_model: str = "",
        _regime: str = "",
        _method: str = "",
        _split_mode: str = "",
        _num_windows: int = 0,
        _is1_ratio: float = 0.0,
        _is2_ratio: float = 0.0,
        _oos_ratio: float = 0.0,
        _start_datetime: str = "",
        _end_datetime: str = "",
        _num_param_combos: int = 0,
        _optimization_metric: str = "",
        _ranking_metric: str = "",
        _notes: str = "",
    ) -> Path:
        """
        Append one row to the persistent simulation_versions.xlsx file.
        
        This file is NEVER deleted between runs — only appended to.
        It contains NO results/metrics, only simulation identification info.
        """
        version_file = self._export_dir / "simulation_versions.xlsx"
        
        row = {
            'version': self.version_prefix,
            'run_timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'instrument': _instrument,
            'timeframe': _timeframe,
            'strategy': _strategy_name,
            'optim_type': _optim_type,
            'regime_model': _regime_model,
            'regime': _regime,
            'method': _method,
            'split_mode': _split_mode,
            'num_windows': _num_windows,
            'is1_ratio': _is1_ratio,
            'is2_ratio': _is2_ratio,
            'oos_ratio': _oos_ratio,
            'start_datetime': _start_datetime,
            'end_datetime': _end_datetime,
            'num_param_combos': _num_param_combos,
            'optimization_metric': _optimization_metric,
            'ranking_metric': _ranking_metric,
            'notes': _notes,
        }
        
        try:
            if version_file.exists():
                existing_df = pd.read_excel(version_file)
                new_df = pd.concat([existing_df, pd.DataFrame([row])], ignore_index=True)
            else:
                new_df = pd.DataFrame([row])
            new_df.to_excel(version_file, index=False)
        except Exception as e:
            # fallback to CSV if openpyxl fails
            csv_fallback = self._export_dir / "simulation_versions.csv"
            try:
                if csv_fallback.exists():
                    existing_df = pd.read_csv(csv_fallback)
                    new_df = pd.concat([existing_df, pd.DataFrame([row])], ignore_index=True)
                else:
                    new_df = pd.DataFrame([row])
                new_df.to_csv(csv_fallback, index=False)
                print(f"  Warning: Excel write failed ({e}), saved to CSV fallback")
            except Exception as e2:
                print(f"  Error: Could not save simulation versions: {e2}")
        
        print(f"  Simulation version {self.version_prefix} logged to {version_file}")
        return version_file
    
    # ------------------------------------------------------------------
    # Transposed simulation results (metrics as rows, combos as columns)
    # ------------------------------------------------------------------
    
    def export_simulation_results(
        self,
        _result: Any,
        _suffix: str = "",
        _top_n: int = 20,
        _include_all_sheet: bool = True,
    ) -> str:
        """
        Export simulation results in TRANSPOSED format (metrics as rows).
        
        Sheet 'Summary': simulation config (metrics as rows, single value column).
        Sheet 'Top Results': metrics as rows, one column per rank (rank_1, rank_2, ...).
        Sheet 'All Results' (optional): same transposed format for all param combos.
        
        Returns:
            Path to the exported xlsx file.
        """
        output_path = self.get_results_path(_suffix) if _suffix else self.get_results_path("results")
        os.makedirs(os.path.dirname(output_path) or '.', exist_ok=True)
        
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            # --- Sheet 1: Summary (config info as rows) ---
            summary_rows = self._build_summary_rows(_result)
            summary_df = pd.DataFrame(summary_rows, columns=['Property', 'Value'])
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
            
            # --- Sheet 2: Top Results (transposed) ---
            if hasattr(_result, 'top_results') and _result.top_results:
                top_df = self._build_transposed_results(_result.top_results[:_top_n], _result)
                top_df.to_excel(writer, sheet_name='Top Results', index=False)
            
            # --- Sheet 3: All Results (transposed) ---
            if _include_all_sheet and hasattr(_result, 'param_results') and _result.param_results:
                all_sorted = sorted(
                    _result.param_results,
                    key=lambda r: getattr(r, 'combined_ranking_score', 0),
                    reverse=True
                )
                all_df = self._build_transposed_results(all_sorted, _result)
                all_df.to_excel(writer, sheet_name='All Results', index=False)
                print(f"  All Results sheet: {len(all_sorted)} parameter combinations exported")
        
        print(f"  Results exported to: {output_path}")
        return output_path
    
    def _build_summary_rows(self, _result: Any) -> list:
        """Build summary config rows [Property, Value]."""
        rows = [
            ['Version', self.version_prefix],
            ['Run Timestamp', datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
            ['Strategy', getattr(_result, 'strategy_name', '')],
            ['Symbol', getattr(_result, 'symbol', '')],
            ['Timeframe', getattr(_result, 'timeframe', '')],
            ['Method', getattr(_result, 'method', '')],
            ['Split Mode', 'IS1+IS2+OOS' if getattr(_result, 'split_mode', 2) == 3 else 'IS+OOS'],
            ['IS1 Ratio', f"{getattr(_result, 'is1_ratio', 0):.0%}"],
            ['IS2 Ratio', f"{getattr(_result, 'is2_ratio', 0):.0%}"],
            ['OOS Ratio', f"{getattr(_result, 'oos_ratio', 0):.0%}"],
            ['Optimization Metric', getattr(_result, 'optimization_metric', '')],
            ['Ranking Metric', getattr(_result, 'ranking_metric', '')],
            ['Total Combinations', getattr(_result, 'total_combinations', 0)],
            ['Passing IS1', getattr(_result, 'passing_is1_constraints', 0)],
            ['Passing IS2', getattr(_result, 'passing_is2_constraints', 0)],
            ['Top Results', len(getattr(_result, 'top_results', []))],
        ]
        
        # IS1 constraints
        c = getattr(_result, 'constraints', None)
        if c:
            rows.extend([
                ['', ''],
                ['--- IS1 Constraints ---', ''],
                ['IS1 Min Trades', c.min_trades],
                ['IS1 Min PF', c.min_profit_factor],
                ['IS1 Min RF', c.min_recovery_factor if c.min_recovery_factor else 'N/A'],
                ['IS1 Max DD%', f"{c.max_drawdown_pct:.1f}%" if c.max_drawdown_pct else 'N/A'],
            ])
        
        # IS2 constraints
        c2 = getattr(_result, 'constraints_is2', None)
        if c2:
            rows.extend([
                ['--- IS2 Constraints (scaled) ---', ''],
                ['IS2 Min Trades', c2.min_trades],
                ['IS2 Min PF', c2.min_profit_factor],
                ['IS2 Min RF', c2.min_recovery_factor if c2.min_recovery_factor else 'N/A'],
                ['IS2 Max DD%', f"{c2.max_drawdown_pct:.1f}%" if c2.max_drawdown_pct else 'N/A'],
            ])
        
        # period info
        sd = getattr(_result, 'start_datetime', '')
        ed = getattr(_result, 'end_datetime', '')
        if sd or ed:
            rows.extend([
                ['', ''],
                ['Start DateTime', sd],
                ['End DateTime', ed],
            ])
        
        return rows
    
    def _build_transposed_results(self, _results: list, _parent_result: Any) -> pd.DataFrame:
        """
        Build transposed DataFrame: metric names as rows, one column per param combination.
        
        Columns: ['Metric', 'rank_1', 'rank_2', ..., 'rank_N']
        """
        rm = getattr(_parent_result, 'ranking_metric', 'upi')
        
        # define the metric rows
        metric_keys = [
            ('Rank', lambda pr, i: i + 1),
            ('Param ID', lambda pr, i: pr.param_id),
            ('Parameters', lambda pr, i: str(pr.params)),
            ('', lambda pr, i: ''),
            ('--- IS1 ---', lambda pr, i: ''),
            (f'IS1 {rm}', lambda pr, i: _safe_get(pr, f'is1_{rm}', 0)),
            ('IS1 PF', lambda pr, i: pr.is1_profit_factor),
            ('IS1 Net Profit', lambda pr, i: pr.is1_net_profit),
            ('IS1 Gross Profit', lambda pr, i: _safe_get(pr, 'is1_gross_profit', 0)),
            ('IS1 Gross Loss', lambda pr, i: _safe_get(pr, 'is1_gross_loss', 0)),
            ('IS1 Max DD %', lambda pr, i: pr.is1_max_drawdown_pct),
            ('IS1 Total Trades', lambda pr, i: pr.is1_total_trades),
            ('IS1 Win Rate %', lambda pr, i: pr.is1_win_rate),
            ('IS1 Avg Risk-Adj Monthly', lambda pr, i: pr.is1_avg_risk_adj_monthly_return),
            ('IS1 Passes', lambda pr, i: 'Y' if pr.passes_is1_constraints else 'N'),
            ('', lambda pr, i: ''),
            ('--- IS2 ---', lambda pr, i: ''),
            (f'IS2 {rm}', lambda pr, i: _safe_get(pr, f'is2_{rm}', 0)),
            ('IS2 PF', lambda pr, i: pr.is2_profit_factor),
            ('IS2 Net Profit', lambda pr, i: pr.is2_net_profit),
            ('IS2 Gross Profit', lambda pr, i: _safe_get(pr, 'is2_gross_profit', 0)),
            ('IS2 Gross Loss', lambda pr, i: _safe_get(pr, 'is2_gross_loss', 0)),
            ('IS2 Max DD %', lambda pr, i: pr.is2_max_drawdown_pct),
            ('IS2 Total Trades', lambda pr, i: pr.is2_total_trades),
            ('IS2 Win Rate %', lambda pr, i: pr.is2_win_rate),
            ('IS2 Avg Risk-Adj Monthly', lambda pr, i: pr.is2_avg_risk_adj_monthly_return),
            ('IS2 Passes', lambda pr, i: 'Y' if pr.passes_is2_constraints else 'N'),
            ('', lambda pr, i: ''),
            ('--- OOS ---', lambda pr, i: ''),
            (f'OOS {rm}', lambda pr, i: _safe_get(pr, f'oos_{rm}', 0)),
            ('OOS PF', lambda pr, i: pr.oos_profit_factor),
            ('OOS Net Profit', lambda pr, i: pr.oos_net_profit),
            ('OOS Gross Profit', lambda pr, i: _safe_get(pr, 'oos_gross_profit', 0)),
            ('OOS Gross Loss', lambda pr, i: _safe_get(pr, 'oos_gross_loss', 0)),
            ('OOS Max DD %', lambda pr, i: pr.oos_max_drawdown_pct),
            ('OOS Total Trades', lambda pr, i: pr.oos_total_trades),
            ('OOS Win Rate %', lambda pr, i: pr.oos_win_rate),
            ('OOS Avg Risk-Adj Monthly', lambda pr, i: pr.oos_avg_risk_adj_monthly_return),
            ('', lambda pr, i: ''),
            ('--- Ranking / Validation ---', lambda pr, i: ''),
            ('Combined Ranking Score', lambda pr, i: pr.combined_ranking_score),
            ('Efficiency IS1>IS2', lambda pr, i: _safe_get(pr, 'efficiency_is1_is2', 0)),
            ('Efficiency IS2>OOS', lambda pr, i: _safe_get(pr, 'efficiency_is2_oos', _safe_get(pr, 'efficiency_ratio', 0))),
            ('Efficiency IS1>OOS', lambda pr, i: _safe_get(pr, 'efficiency_is1_oos', 0)),
            ('Consistency IS1>IS2', lambda pr, i: _safe_get(pr, 'consistency_is1_is2', 0)),
            ('Consistency IS2>OOS', lambda pr, i: _safe_get(pr, 'consistency_is2_oos', _safe_get(pr, 'consistency_score', 0))),
            ('Consistency IS1>OOS', lambda pr, i: _safe_get(pr, 'consistency_is1_oos', 0)),
            ('Sensitivity +/-10%', lambda pr, i: _safe_get(pr, 'parameter_sensitivity_10', 0)),
            ('Sensitivity +/-20%', lambda pr, i: _safe_get(pr, 'parameter_sensitivity_20', 0)),
            ('Cross-Win Stability', lambda pr, i: pr.cross_windows_stability),
        ]
        
        # expand parameters into individual rows
        param_rows = self._expand_param_rows(_results)
        
        # build the data dict
        data = {'Metric': [mk[0] for mk in metric_keys]}
        
        for i, pr in enumerate(_results):
            col_name = f"rank_{i+1:03d}"
            col_values = []
            for _, extractor in metric_keys:
                try:
                    val = extractor(pr, i)
                    col_values.append(val)
                except Exception:
                    col_values.append('')
            data[col_name] = col_values
        
        # append expanded parameter rows
        if param_rows:
            for pkey in param_rows:
                data['Metric'].append(f'  param: {pkey}')
                for i, pr in enumerate(_results):
                    col_name = f"rank_{i+1:03d}"
                    params = dict(pr.params) if isinstance(pr.params, dict) else {}
                    data[col_name].append(params.get(pkey, ''))
        
        return pd.DataFrame(data)
    
    def _expand_param_rows(self, _results: list) -> list:
        """Extract unique parameter keys from all results."""
        all_keys = set()
        for pr in _results:
            params = dict(pr.params) if isinstance(pr.params, dict) else {}
            all_keys.update(params.keys())
        return sorted(all_keys)
    
    # ------------------------------------------------------------------
    # WF window details (windows as columns, metrics as rows)
    # ------------------------------------------------------------------
    
    def export_wf_window_details(
        self,
        _result: Any,
        _suffix: str = "wf_windows",
    ) -> str:
        """
        Export walk-forward window details in transposed format.
        
        Rows: metric names.
        Columns: window_1, window_2, ..., window_N.
        
        Works for both standard WF (WalkForwardResult) and advanced WF (AdvancedWFResult).
        
        Returns:
            Path to exported xlsx file.
        """
        output_path = self.get_wf_details_path(_suffix)
        os.makedirs(os.path.dirname(output_path) or '.', exist_ok=True)
        
        # detect result type and extract windows
        if hasattr(_result, 'window_results') and _result.window_results:
            # standard WalkForwardResult
            windows = _result.window_results
            df = self._build_standard_wf_transposed(windows)
        elif hasattr(_result, 'top_results') and _result.top_results:
            # advanced WF: per-window data for best result across windows
            # for single-window mode, the top_results themselves ARE the windows
            df = self._build_advanced_wf_transposed(_result)
        else:
            df = pd.DataFrame({'Metric': ['No window data available'], 'Info': ['']})
        
        df.to_excel(output_path, index=False)
        print(f"  WF window details exported to: {output_path}")
        return output_path
    
    def _build_standard_wf_transposed(self, _windows: list) -> pd.DataFrame:
        """Build transposed DF for standard WalkForwardResult windows."""
        metric_defs = [
            ('Window ID', lambda wr, i: getattr(getattr(wr, 'window', None), 'window_id', i+1)),
            ('Train Start', lambda wr, i: getattr(getattr(wr, 'window', None), 'train_start', '')),
            ('Train End', lambda wr, i: getattr(getattr(wr, 'window', None), 'train_end', '')),
            ('Test Start', lambda wr, i: getattr(getattr(wr, 'window', None), 'test_start', '')),
            ('Test End', lambda wr, i: getattr(getattr(wr, 'window', None), 'test_end', '')),
            ('Best Params', lambda wr, i: str(getattr(wr, 'best_params', {}))),
            ('', lambda wr, i: ''),
            ('--- Train Metrics ---', lambda wr, i: ''),
            ('Train Net Profit', lambda wr, i: _nested_attr(wr, 'train_result', 'net_profit', 0)),
            ('Train PF', lambda wr, i: _nested_attr(wr, 'train_result', 'profit_factor', 0)),
            ('Train Sharpe', lambda wr, i: _nested_attr(wr, 'train_result', 'sharpe_ratio', 0)),
            ('Train Max DD %', lambda wr, i: _nested_attr(wr, 'train_result', 'max_drawdown_pct', 0)),
            ('Train Total Trades', lambda wr, i: _nested_attr(wr, 'train_result', 'total_trades', 0)),
            ('', lambda wr, i: ''),
            ('--- Test Metrics ---', lambda wr, i: ''),
            ('Test Net Profit', lambda wr, i: _nested_attr(wr, 'test_result', 'net_profit', 0)),
            ('Test PF', lambda wr, i: _nested_attr(wr, 'test_result', 'profit_factor', 0)),
            ('Test Sharpe', lambda wr, i: _nested_attr(wr, 'test_result', 'sharpe_ratio', 0)),
            ('Test Max DD %', lambda wr, i: _nested_attr(wr, 'test_result', 'max_drawdown_pct', 0)),
            ('Test Win Rate', lambda wr, i: _nested_attr(wr, 'test_result', 'win_rate', 0)),
            ('Test Total Trades', lambda wr, i: _nested_attr(wr, 'test_result', 'total_trades', 0)),
        ]
        
        data = {'Metric': [m[0] for m in metric_defs]}
        for i, wr in enumerate(_windows):
            col = f"window_{i+1:03d}"
            data[col] = [m[1](wr, i) for m in metric_defs]
        
        return pd.DataFrame(data)
    
    def _build_advanced_wf_transposed(self, _result: Any) -> pd.DataFrame:
        """
        Build transposed DF for advanced WF results.
        
        For multi-window mode: each window is a column.
        For single-window mode: show the per-window breakdown from window_results if available.
        """
        # check if multi-window data is available
        window_results = getattr(_result, 'window_results', None)
        if window_results and len(window_results) > 1:
            return self._build_advanced_multi_window_transposed(window_results, _result)
        
        # single-window mode: show top results as "windows" (one per param combo)
        # This is less about windows and more about showing that no multi-window detail exists
        data = {
            'Metric': [
                'Mode', 'Note',
                '', '--- Top Result Details ---',
            ],
            'Info': [
                'Single window (or regime segment)',
                'No multi-window breakdown available for single-window mode.',
                '', '',
            ]
        }
        
        # still show top-1 details if available
        if _result.top_results:
            top = _result.top_results[0]
            data['Metric'].extend([
                'Best Params', 
                'IS1 Net Profit', 'IS1 PF', 'IS1 UPI',
                'IS2 Net Profit', 'IS2 PF', 'IS2 UPI',
                'OOS Net Profit', 'OOS PF', 'OOS UPI',
                'Ranking Score',
            ])
            data['Info'].extend([
                str(top.params),
                top.is1_net_profit, top.is1_profit_factor, top.is1_upi,
                top.is2_net_profit, top.is2_profit_factor, top.is2_upi,
                top.oos_net_profit, top.oos_profit_factor, top.oos_upi,
                top.combined_ranking_score,
            ])
        
        return pd.DataFrame(data)
    
    def _build_advanced_multi_window_transposed(self, _window_results: list, _result: Any) -> pd.DataFrame:
        """Build transposed DF for advanced WF multi-window results."""
        rm = getattr(_result, 'ranking_metric', 'upi')
        
        metric_defs = [
            ('Window ID', lambda wr, i: getattr(wr, 'window_id', i+1)),
            ('Start', lambda wr, i: getattr(wr, 'start_datetime', '')),
            ('End', lambda wr, i: getattr(wr, 'end_datetime', '')),
            ('IS1 Bars', lambda wr, i: getattr(wr, 'is1_bars', 0)),
            ('IS2 Bars', lambda wr, i: getattr(wr, 'is2_bars', 0)),
            ('OOS Bars', lambda wr, i: getattr(wr, 'oos_bars', 0)),
            ('', lambda wr, i: ''),
            ('Best Params', lambda wr, i: str(getattr(wr, 'best_params', {}))),
            (f'IS1 {rm}', lambda wr, i: _safe_get(wr, f'is1_{rm}', 0)),
            ('IS1 PF', lambda wr, i: _safe_get(wr, 'is1_profit_factor', 0)),
            ('IS1 Trades', lambda wr, i: _safe_get(wr, 'is1_total_trades', 0)),
            (f'IS2 {rm}', lambda wr, i: _safe_get(wr, f'is2_{rm}', 0)),
            ('IS2 PF', lambda wr, i: _safe_get(wr, 'is2_profit_factor', 0)),
            ('IS2 Trades', lambda wr, i: _safe_get(wr, 'is2_total_trades', 0)),
            (f'OOS {rm}', lambda wr, i: _safe_get(wr, f'oos_{rm}', 0)),
            ('OOS PF', lambda wr, i: _safe_get(wr, 'oos_profit_factor', 0)),
            ('OOS Net Profit', lambda wr, i: _safe_get(wr, 'oos_net_profit', 0)),
            ('OOS Max DD %', lambda wr, i: _safe_get(wr, 'oos_max_drawdown_pct', 0)),
            ('OOS Trades', lambda wr, i: _safe_get(wr, 'oos_total_trades', 0)),
            ('Ranking Score', lambda wr, i: _safe_get(wr, 'combined_ranking_score', 0)),
        ]
        
        data = {'Metric': [m[0] for m in metric_defs]}
        for i, wr in enumerate(_window_results):
            col = f"window_{i+1:03d}"
            data[col] = []
            for _, extractor in metric_defs:
                try:
                    data[col].append(extractor(wr, i))
                except Exception:
                    data[col].append('')
        
        return pd.DataFrame(data)
    
    # ------------------------------------------------------------------
    # Standard WF export (for section 5.2c)
    # ------------------------------------------------------------------
    
    def export_standard_wf_results(
        self,
        _wf_result: Any,
        _suffix: str = "",
    ) -> str:
        """
        Export standard walk-forward results (2-split IS/OOS).
        
        Sheet 'Summary': aggregated metrics (transposed).
        Sheet 'Windows': per-window details (transposed, windows as columns).
        """
        suffix = _suffix or f"{_wf_result.strategy_name.replace('Strategy', '')}_{_wf_result.symbol}_{_wf_result.timeframe}_wf"
        output_path = self.get_results_path(suffix)
        os.makedirs(os.path.dirname(output_path) or '.', exist_ok=True)
        
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            # Summary sheet
            summary_rows = [
                ['Version', self.version_prefix],
                ['Strategy', _wf_result.strategy_name],
                ['Symbol', _wf_result.symbol],
                ['Timeframe', _wf_result.timeframe],
                ['Method', getattr(_wf_result, 'method', '')],
                ['Num Windows', getattr(_wf_result, 'num_windows', 0)],
                ['Best Params', str(getattr(_wf_result, 'best_params', {}))],
                ['', ''],
                ['--- Aggregated OOS Metrics ---', ''],
                ['Net Profit', getattr(_wf_result, 'net_profit_agg', 0)],
                ['Profit Factor', getattr(_wf_result, 'profit_factor_agg', 0)],
                ['Recovery Factor', getattr(_wf_result, 'recovery_factor_agg', 0)],
                ['Max Drawdown', getattr(_wf_result, 'max_drawdown_agg', 0)],
                ['Win Rate', getattr(_wf_result, 'win_rate_agg', 0)],
                ['Total Trades', getattr(_wf_result, 'total_trades_agg', 0)],
                ['Sharpe Ratio', getattr(_wf_result, 'sharpe_agg', 0)],
                ['Passes Filter', getattr(_wf_result, 'passes_filter', '')],
            ]
            pd.DataFrame(summary_rows, columns=['Property', 'Value']).to_excel(
                writer, sheet_name='Summary', index=False)
            
            # Window details sheet (transposed)
            if hasattr(_wf_result, 'window_results') and _wf_result.window_results:
                wf_df = self._build_standard_wf_transposed(_wf_result.window_results)
                wf_df.to_excel(writer, sheet_name='Windows', index=False)
        
        print(f"  Standard WF results exported to: {output_path}")
        return output_path
    
    # ------------------------------------------------------------------
    # Regime backtest export (for section 6.1d)
    # ------------------------------------------------------------------
    
    def export_regime_backtest_results(
        self,
        _regime_results: dict,
        _suffix: str = "regime_backtest",
    ) -> str:
        """Export regime backtest results (transposed: metrics as rows, regimes as columns)."""
        output_path = self.get_results_path(_suffix)
        
        regime_names = {1: "Uptrend", -1: "Downtrend", 0: "Range"}
        
        metric_defs = [
            ('Net Profit', lambda r: getattr(r, 'net_profit', 0)),
            ('Gross Profit', lambda r: getattr(r, 'gross_profit', 0)),
            ('Gross Loss', lambda r: getattr(r, 'gross_loss', 0)),
            ('Profit Factor', lambda r: getattr(r, 'profit_factor', 0)),
            ('Recovery Factor', lambda r: getattr(r, 'recovery_factor', 0)),
            ('Win Rate %', lambda r: getattr(r, 'win_rate', 0)),
            ('Total Trades', lambda r: getattr(r, 'total_trades', 0)),
            ('Max Drawdown', lambda r: getattr(r, 'max_drawdown', 0)),
            ('Max Drawdown %', lambda r: getattr(r, 'max_drawdown_pct', 0)),
            ('Sharpe Ratio', lambda r: getattr(r, 'sharpe_ratio', 0)),
        ]
        
        data = {'Metric': [m[0] for m in metric_defs]}
        for regime_val, result in _regime_results.items():
            col_name = regime_names.get(regime_val, f"Regime_{regime_val}")
            data[col_name] = [m[1](result) for m in metric_defs]
        
        df = pd.DataFrame(data)
        df.to_excel(output_path, index=False)
        print(f"  Regime backtest results exported to: {output_path}")
        return output_path

    def delete_prev_charts(self, _version_prefix: str = None) -> int:
        """Delete previous chart files for the given version prefix.
        
        Args:
            _version_prefix: Version prefix to match (default: self.version_prefix).
        Returns:
            Number of files deleted.
        """
        prefix = _version_prefix or self._version_prefix
        count = 0
        for d in [self._charts_dir, self._dynamic_charts_dir]:
            if d.exists():
                for f in d.iterdir():
                    if f.is_file() and f.name.startswith(prefix) and f.suffix in ('.jpg', '.jpeg', '.png', '.html'):
                        f.unlink()
                        count += 1
        return count


# ======================================================================
# Module-level helpers
# ======================================================================

def _safe_get(obj, attr, default=0):
    """Safely get attribute from object."""
    return getattr(obj, attr, default)

def _nested_attr(obj, parent_attr, child_attr, default=0):
    """Get nested attribute: obj.parent_attr.child_attr."""
    parent = getattr(obj, parent_attr, None)
    if parent is None:
        return default
    return getattr(parent, child_attr, default)
