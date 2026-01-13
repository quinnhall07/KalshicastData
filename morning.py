from __future__ import annotations

import random
import time
from datetime import date, datetime
from typing import Any, Dict, List

import requests

from config import STATIONS
from sources_registry import load_fetchers_safe
from db import upsert_forecast, upsert_source, upsert_station, init_db


MAX_ATTEMPTS = 4
BASE_SLEEP_SECONDS = 2.0  # backoff base


def _coerce_float(x: Any) -> float:
    if isinstance(x, (int, float)):
        return float(x)
    if isinstance(x, str):
        return float(x.strip())
    raise ValueError(f"not a number: {x!r}")


def _normalize_rows(rows: Any) -> List[Dict[str, Any]]:
    """
    Enforce standardized output:
      [{"target_date":"YYYY-MM-DD","high":float,"low":float}, ...]
    Filters out malformed entries instead of crashing the whole run.
    """
    if rows is None:
        return []
    if not isinstance(rows, list):
        raise ValueError(f"fetcher returned {type(rows)}; expected list[dict]")

    out: List[Dict[str, Any]] = []
    for i, r in enumerate(rows):
        if not isinstance(r, dict):
            print(f"[morning] WARN: skipping non-dict row at index {i}: {r!r}")
            continue
        if "target_date" not in r or "high" not in r or "low" not in r:
            print(f"[morning] WARN: skipping incomplete row at index {i}: {r!r}")
            continue
        try:
            target_date = str(r["target_date"])[:10]
            high = _coerce_float(r["high"])
            low = _coerce_float(r["low"])
        except Exception as e:
            print(f"[morning] WARN: skipping bad row at index {i}: {r!r} ({e})")
            continue

        out.append({"target_date": target_date, "high": high, "low": low})
    return out


def _is_retryable_error(e: Exception) -> bool:
    # Network-y stuff
    if isinstance(e, (requests.Timeout, requests.ConnectionError)):
        return True

    # requests wraps HTTP status failures as HTTPError with response attached
    if isinstance(e, requests.HTTPError):
        resp = getattr(e, "response", None)
        code = getattr(resp, "status_code", None)
        if code is None:
            return True
        # retry 429 + 5xx
        return code == 429 or code >= 500

    # Tomorrow/other libs sometimes throw generic RuntimeError for 5xx;
    # we conservatively retry a couple times for common transient words.
    msg = str(e).lower()
    transient_hints = [
        "timed out",
        "timeout",
        "temporarily",
        "try again",
        "connection reset",
        "connection aborted",
        "service unavailable",
        "internal server error",
        "bad gateway",
        "gateway timeout",
        "too many requests",
        "rate limit",
    ]
    return any(h in msg for h in transient_hints)


def _call_fetcher_with_retry(fetcher, station: dict, source_id: str) -> Any:
    last_exc: Exception | None = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            return fetcher(station)
        except Exception as e:
            last_exc = e
            if attempt >= MAX_ATTEMPTS or not _is_retryable_error(e):
                raise
            sleep_s = (BASE_SLEEP_SECONDS * attempt) + random.random() * 0.5
            print(f"[morning] RETRY {station['station_id']} {source_id} attempt {attempt}/{MAX_ATTEMPTS} after error: {e}")
            time.sleep(sleep_s)
    raise last_exc  # unreachable


def main() -> None:
    init_db()

    # Ensure stations exist (FKs)
    for st in STATIONS:
        upsert_station(st["station_id"], st.get("name"), st.get("lat"), st.get("lon"))

    # Load enabled fetchers safely (won't crash if one import fails)
    fetchers = load_fetchers_safe()
    if not fetchers:
        print("[morning] ERROR: no enabled sources loaded (check config.SOURCES).")
        return

    # Ensure sources exist (FKs)
    for source_id in fetchers.keys():
        upsert_source(source_id)

    forecast_date = date.today().isoformat()

    for st in STATIONS:
        station_id = st["station_id"]

        for source_id, fetcher in fetchers.items():
            fetched_at = datetime.now().isoformat(timespec="seconds")

            try:
                raw_rows = _call_fetcher_with_retry(fetcher, st, source_id)
                rows = _normalize_rows(raw_rows)

                if not rows:
                    print(f"[morning] WARN {station_id} {source_id}: no rows returned")
                    continue

                for f in rows:
                    upsert_forecast(
                        station_id=station_id,
                        source_id=source_id,
                        forecast_date=forecast_date,
                        target_date=f["target_date"],
                        high=f["high"],
                        low=f["low"],
                        fetched_at=fetched_at,
                    )

                print(f"[morning] OK {station_id} {source_id}: saved {len(rows)} row(s)")
            except Exception as e:
                print(f"[morning] FAIL {station_id} {source_id}: {e}")
            if source_id.startswith("OME_"):
                time.sleep(0.4 + random.random() * 0.6)



if __name__ == "__main__":
    main()

