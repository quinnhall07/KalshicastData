# sources_registry.py
from __future__ import annotations

import importlib
from typing import Any, Callable, Dict

from config import SOURCES

# A fetcher returns either:
#  - list[dict]                              (legacy daily rows)
#  - dict with keys:
#       issued_at: str
#       rows: list[dict]                    (daily)
#       hourly_rows OR hourly (optional)    (hourly, unaggregated)
Fetcher = Callable[[dict], Any]


def load_fetchers_safe() -> Dict[str, Fetcher]:
    """
    Loads enabled sources and returns a mapping:
        source_id -> fetcher(station) -> payload

    Key guarantees:
    - params are bound correctly per source (no late-binding bug)
    - fetchers are wrapped uniformly
    - does NOT mutate payloads (morning.py owns normalization)
    """
    out: Dict[str, Fetcher] = {}

    for source_id, spec in (SOURCES or {}).items():
        if not spec.get("enabled"):
            continue

        mod_name = spec["module"]
        fn_name = spec["func"]
        params = spec.get("params") or {}

        mod = importlib.import_module(mod_name)
        fn = getattr(mod, fn_name)

        if not callable(fn):
            raise TypeError(f"Fetcher {mod_name}.{fn_name} is not callable")

        # Bind params safely (avoid closure capture bugs)
        if params:
            def make_fetcher(f, p):
                def _fetch(station: dict):
                    return f(station, p)
                return _fetch
            out[source_id] = make_fetcher(fn, params)
        else:
            def make_fetcher(f):
                def _fetch(station: dict):
                    return f(station)
                return _fetch
            out[source_id] = make_fetcher(fn)

    return out
