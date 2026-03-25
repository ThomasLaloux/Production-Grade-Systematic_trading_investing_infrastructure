"""
Instrument Currency Map Module
================================
Auto-derives instrument → [affected_currencies] mapping from instruments.yaml.

Mapping Rules (per asset class):
    - forex:     both currency_base and currency_quote
                 (EURUSD → [EUR, USD])
    - index:     country currency (currency_base)
                 (US100 → [USD])
    - index futures: same as index
    - commodity: quote currency (typically USD)
                 (XAUUSD → [USD])
    - crypto:    quote currency (typically USD)
                 (BTCUSD → [USD])

This module reads instruments.yaml at init time and provides fast lookups.
No separate mapping file is needed — single source of truth is instruments.yaml.

Classes:
    InstrumentCurrencyMap:
        - get_affected_currencies: instrument → list of currencies
        - get_instruments_for_currency: currency → list of instruments
        - get_asset_class: instrument → asset class string
        - get_all_currencies: set of all currencies across all instruments

Usage:
    mapper = InstrumentCurrencyMap(_instruments_path="data/instruments.yaml")
    currencies = mapper.get_affected_currencies("EURUSD", _broker="oanda")
    # → ["EUR", "USD"]
    currencies = mapper.get_affected_currencies("US100", _broker="icm_mt5")
    # → ["USD"]
    currencies = mapper.get_affected_currencies("XAUUSD", _broker="icm_mt5")
    # → ["USD"]
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional, Set

import yaml

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

logger = logging.getLogger(__name__)


class InstrumentCurrencyMap:
    """
    Auto-derives instrument → affected currencies from instruments.yaml.

    Reads instrument definitions once at init, builds an in-memory lookup
    for O(1) queries by instrument + broker.
    """

    def __init__(
        self,
        _instruments_path: str = "data/instruments.yaml",
    ):
        """
        Initialize InstrumentCurrencyMap.

        Args:
            _instruments_path: Path to instruments.yaml.
        """
        self._instruments_path = Path(_instruments_path)
        # {broker: {symbol: {"currencies": [str], "asset_class": str}}}
        self._map: Dict[str, Dict[str, Dict]] = {}
        # Reverse map: {currency: set of (broker, symbol)}
        self._reverse_map: Dict[str, Set] = {}
        self._all_currencies: Set[str] = set()

        self._load()

    def _load(self) -> None:
        """Load instruments.yaml and build mapping."""
        if not self._instruments_path.exists():
            logger.error(
                f"InstrumentCurrencyMap: instruments file not found: "
                f"{self._instruments_path}"
            )
            return

        with open(self._instruments_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}

        instruments_data = data.get("instruments", {})

        for broker, symbols in instruments_data.items():
            self._map[broker] = {}

            for symbol, spec in symbols.items():
                asset_class = spec.get("asset_class", "").lower()
                currency_base = spec.get("currency_base", "").upper()
                currency_quote = spec.get("currency_quote", "").upper()

                affected = self._derive_currencies(
                    asset_class, currency_base, currency_quote,
                )

                self._map[broker][symbol] = {
                    "currencies": affected,
                    "asset_class": asset_class,
                    "currency_base": currency_base,
                    "currency_quote": currency_quote,
                }

                self._all_currencies.update(affected)

                for curr in affected:
                    if curr not in self._reverse_map:
                        self._reverse_map[curr] = set()
                    self._reverse_map[curr].add((broker, symbol))

        total_instruments = sum(len(v) for v in self._map.values())
        logger.info(
            f"InstrumentCurrencyMap: loaded {total_instruments} instruments "
            f"across {len(self._map)} brokers, "
            f"{len(self._all_currencies)} unique currencies"
        )

    @staticmethod
    def _derive_currencies(
        _asset_class: str,
        _currency_base: str,
        _currency_quote: str,
    ) -> List[str]:
        """
        Derive affected currencies based on asset class.

        Args:
            _asset_class: Asset class string (forex, index, commodity, crypto).
            _currency_base: Base currency from instruments.yaml.
            _currency_quote: Quote currency from instruments.yaml.

        Returns:
            List of affected currency codes.
        """
        currencies = []

        if _asset_class == "forex":
            # Both legs matter for news impact
            if _currency_base:
                currencies.append(_currency_base)
            if _currency_quote and _currency_quote != _currency_base:
                currencies.append(_currency_quote)

        elif _asset_class in ("index", "index futures"):
            # Country currency (currency_base for indices is the local currency)
            if _currency_base:
                currencies.append(_currency_base)

        elif _asset_class == "commodity":
            # Quote currency (typically USD for XAUUSD, XAGUSD)
            if _currency_quote:
                currencies.append(_currency_quote)

        elif _asset_class == "crypto":
            # Quote currency (typically USD)
            if _currency_quote:
                currencies.append(_currency_quote)

        else:
            # Fallback: use both
            if _currency_base:
                currencies.append(_currency_base)
            if _currency_quote and _currency_quote != _currency_base:
                currencies.append(_currency_quote)

        return currencies

    def get_affected_currencies(
        self,
        _instrument: str,
        _broker: Optional[str] = None,
    ) -> List[str]:
        """
        Get currencies that affect the given instrument.

        Args:
            _instrument: Instrument symbol (e.g. "EURUSD", "XAUUSDp", "US100").
            _broker: Broker name. If None, searches all brokers.

        Returns:
            List of currency codes (e.g. ["EUR", "USD"]).
        """
        if _broker is not None:
            broker_map = self._map.get(_broker, {})
            entry = broker_map.get(_instrument)
            if entry is not None:
                return entry["currencies"]
            # Fallback: try standard symbol lookup across all brokers
            for b, symbols in self._map.items():
                if _instrument in symbols:
                    return symbols[_instrument]["currencies"]
            return []

        # No broker specified — search all
        for broker, symbols in self._map.items():
            if _instrument in symbols:
                return symbols[_instrument]["currencies"]

        return []

    def get_asset_class(
        self,
        _instrument: str,
        _broker: Optional[str] = None,
    ) -> str:
        """
        Get asset class for the given instrument.

        Args:
            _instrument: Instrument symbol.
            _broker: Broker name. If None, searches all brokers.

        Returns:
            Asset class string (e.g. "forex", "index", "commodity", "crypto").
        """
        if _broker is not None:
            broker_map = self._map.get(_broker, {})
            entry = broker_map.get(_instrument)
            if entry is not None:
                return entry["asset_class"]

        for broker, symbols in self._map.items():
            if _instrument in symbols:
                return symbols[_instrument]["asset_class"]

        return ""

    def get_instruments_for_currency(
        self,
        _currency: str,
        _broker: Optional[str] = None,
    ) -> List[str]:
        """
        Get all instruments affected by a given currency.

        Args:
            _currency: Currency code (e.g. "USD").
            _broker: Filter by broker. None = all brokers.

        Returns:
            List of instrument symbols.
        """
        entries = self._reverse_map.get(_currency.upper(), set())
        if _broker is not None:
            return [sym for b, sym in entries if b == _broker]
        return list(set(sym for _, sym in entries))

    def get_all_currencies(self) -> Set[str]:
        """Get set of all currencies across all instruments."""
        return self._all_currencies.copy()

    def get_all_instruments(
        self, _broker: Optional[str] = None,
    ) -> Dict[str, List[str]]:
        """
        Get all instruments and their affected currencies.

        Args:
            _broker: Filter by broker. None = all.

        Returns:
            Dict of {instrument: [currencies]}.
        """
        result = {}
        brokers = [_broker] if _broker else list(self._map.keys())

        for b in brokers:
            for symbol, entry in self._map.get(b, {}).items():
                result[symbol] = entry["currencies"]

        return result
