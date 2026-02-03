from __future__ import annotations

import importlib
from typing import Any, Callable, Dict, Optional

from config import SOURCES

# Collector contract expected by morning.py (and enforced here):
# {
#   "issued_at": str,
#   "daily": list[{"target_date": "YYYY-MM-DD", "high_f": float, "low_f": float}],
#   "hourly": dict with key "time" -> list[str] (optional)
# }
Fetcher = Callable[[dict], Dict[str, Any]]


def _is_daily_row_ok(r: Any) -> bool:
    return (
        isinstance(r, dict)
        and isinstance(r.get("target_date"), str)
        and (r.get("high_f") is None or isinstance(r.get("high_f"), (int, float)))
        and (r.get("low_f") is None or isinstance(r.get("low_f"), (int, float)))
    )


def _validate_payload(source_id: str, payload: Any) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        raise TypeError(f"[sources_registry] {source_id}: payload must be dict, got {type(payload).__name__}")

    issued_at = payload.get("issued_at")
    if not isinstance(issued_at, str) or not issued_at:
        raise ValueError(f"[sources_registry] {source_id}: missing/invalid issued_at")

    daily = payload.get("daily")
    if not isinstance(daily, list):
        raise ValueError(f"[sources_registry] {source_id}: missing/invalid daily (expected list)")

    for i, r in enumerate(daily):
        if not _is_daily_row_ok(r):
            raise ValueError(f"[sources_registry] {source_id}: invalid daily row at index {i}: {r}")

    hourly = payload.get("hourly", None)
    if hourly is not None:
        if not isinstance(hourly, dict):
            raise ValueError(f"[sources_registry] {source_id}: hourly must be dict (arrays object)")
        t = hourly.get("time")
        if not isinstance(t, list):
            raise ValueError(f"[sources_registry] {source_id}: hourly.time must be list")
        # Do not over-validate variable arrays here; morning.py owns write-time details.

    return payload


def _wrap_fetcher(source_id: str, fn: Callable[..., Any], params: Optional[dict]) -> Fetcher:
    if params:
        def _fetch(station: dict) -> Dict[str, Any]:
            payload = fn(station, params)
            return _validate_payload(source_id, payload)
        return _fetch

    def _fetch(station: dict) -> Dict[str, Any]:
        payload = fn(station)
        return _validate_payload(source_id, payload)
    return _fetch


def load_fetchers_safe() -> Dict[str, Fetcher]:
    """
    Loads enabled sources and returns:
        source_id -> fetcher(station) -> STRICT payload dict

    Guarantees:
    - params bound correctly (no late-binding bug)
    - payload shape validated here (fail fast)
    - no mutation/normalization beyond validation
    """
    out: Dict[str, Fetcher] = {}

    for source_id, spec in (SOURCES or {}).items():
        if not spec.get("enabled"):
            continue

        mod_name = spec["module"]
        fn_name = spec["func"]
        params = spec.get("params") or None

        mod = importlib.import_module(mod_name)
        fn = getattr(mod, fn_name)

        if not callable(fn):
            raise TypeError(f"[sources_registry] {source_id}: {mod_name}.{fn_name} is not callable")

        out[source_id] = _wrap_fetcher(source_id, fn, params)

    return out
