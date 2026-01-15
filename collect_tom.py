# collect_tom.py
from __future__ import annotations

import os
import time
import random
import requests
from datetime import date, datetime, timezone
from typing import Any, Dict, List

from config import HEADERS

TOM_URL = "https://api.tomorrow.io/v4/timelines"


def _get_key() -> str:
    key = os.getenv("TOMORROW_API_KEY")
    if not key:
        raise RuntimeError(
            "Missing TOMORROW_API_KEY env var.\n"
            "PowerShell: $env:TOMORROW_API_KEY='...'\n"
            "macOS/Linux: export TOMORROW_API_KEY='...'"
        )
    return key


def _post_with_retry(
    url: str,
    *,
    params: dict,
    json: dict,
    headers: dict,
    timeout: int = 20,
):
    for attempt in (1, 2):
        r = requests.post(url, params=params, json=json, headers=headers, timeout=timeout)
        if r.status_code < 500 and r.status_code != 429:
            r.raise_for_status()
            return r
        if attempt == 2:
            r.raise_for_status()
        sleep_s = (0.75 * attempt) + random.random() * 0.25
        time.sleep(sleep_s)
    raise RuntimeError("unreachable")


def fetch_tom_forecast(station: dict, params: Dict[str, Any] | None = None) -> Dict[str, Any]:
    """
    Tomorrow.io daily forecast -> standardized output
    Returns:
      {"issued_at":"...Z","rows":[{"target_date":"YYYY-MM-DD","high":..,"low":.., extras...}, ...]}
    """
    lat = station.get("lat")
    lon = station.get("lon")
    if lat is None or lon is None:
        raise ValueError("Tomorrow.io fetch requires station['lat'] and station['lon'].")

    params = params or {}
    ndays = int(params.get("days", 2))
    units = params.get("units", "imperial")

    today = date.today()
    end = date.fromordinal(today.toordinal() + (ndays - 1))

    payload = {
        "location": f"{float(lat)},{float(lon)}",
        "fields": [
            "temperatureMax",
            "temperatureMin",
            "dewPointAvg",
            "humidityAvg",
            "windSpeedAvg",
            "windDirectionAvg",
            "cloudCoverAvg",
            "precipitationProbabilityAvg",
        ],
        "timesteps": ["1d"],
        "units": units,
        "startTime": today.isoformat(),
        "endTime": end.isoformat(),
    }

    r = _post_with_retry(
        TOM_URL,
        params={"apikey": _get_key()},
        json=payload,
        headers=dict(HEADERS),
        timeout=20,
    )
    data = r.json()

    issued_at = (
        (data.get("data") or {}).get("time")
        or (data.get("data") or {}).get("timestep")  # fallback (rare)
    )
    if isinstance(issued_at, str) and issued_at.endswith("Z"):
        issued_at = issued_at
    else:
        issued_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    timelines = (data.get("data") or {}).get("timelines") or []
    if not timelines:
        raise RuntimeError("Tomorrow.io returned no timelines")

    intervals = timelines[0].get("intervals") or []
    if not intervals:
        raise RuntimeError("Tomorrow.io returned empty daily intervals")

    allowed = {
        today.isoformat(),
        date.fromordinal(today.toordinal() + 1).isoformat(),
    }

    rows: List[dict] = []
    for iv in intervals:
        start = iv.get("startTime")
        values = iv.get("values") or {}
        if not start:
            continue

        d = start[:10]
        if d not in allowed:
            continue

        hi = values.get("temperatureMax")
        lo = values.get("temperatureMin")
        if hi is None or lo is None:
            continue

        row: Dict[str, Any] = {"target_date": d, "high": float(hi), "low": float(lo)}

        if values.get("dewPointAvg") is not None:
            row["dewpoint_f"] = float(values["dewPointAvg"])
        if values.get("humidityAvg") is not None:
            row["humidity_pct"] = float(values["humidityAvg"])
        if values.get("windSpeedAvg") is not None:
            row["wind_speed_mph"] = float(values["windSpeedAvg"])
        if values.get("windDirectionAvg") is not None:
            row["wind_dir_deg"] = float(values["windDirectionAvg"])
        if values.get("cloudCoverAvg") is not None:
            row["cloud_cover_pct"] = float(values["cloudCoverAvg"])
        if values.get("precipitationProbabilityAvg") is not None:
            row["precip_prob_pct"] = float(values["precipitationProbabilityAvg"])

        rows.append(row)

    return {"issued_at": issued_at, "rows": rows}
