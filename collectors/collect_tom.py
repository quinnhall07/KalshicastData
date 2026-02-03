# collect_tom.py
from __future__ import annotations

import os
from datetime import date, datetime, timezone
from typing import Any, Dict, List

import requests

from config import HEADERS

TOM_URL = "https://api.tomorrow.io/v4/timelines"


def _utc_now_z() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def fetch_tom_forecast(station: dict, params: Dict[str, Any] | None = None) -> Dict[str, Any]:
    """
    Strict payload shape:
      {
        "issued_at": "...Z",
        "rows": [
          {"target_date": "YYYY-MM-DD", "high": float, "low": float, "extras": {...}},
          ...
        ]
      }

    Notes:
      - Default horizon: 3 days (today + next 2 days)
      - Extras are daily aggregates from Tomorrow.io if available.
    """
    params = params or {}

    lat = station.get("lat")
    lon = station.get("lon")
    if lat is None or lon is None:
        raise ValueError("Tomorrow.io fetch requires station['lat'] and station['lon'].")

    key = os.getenv("TOMORROW_API_KEY")
    if not key:
        raise RuntimeError("Missing TOMORROW_API_KEY env var")

    # Default to 3-day horizon (today + next 2 days)
    ndays = int(params.get("days", 3))
    if ndays < 1:
        ndays = 1

    today = date.today()
    end = date.fromordinal(today.toordinal() + (ndays - 1))
    want = {date.fromordinal(today.toordinal() + i).isoformat() for i in range(ndays)}

    # Daily fields (temps required; extras optional)
    fields = [
        "temperatureMax",
        "temperatureMin",
        "humidityAvg",
        "windSpeedAvg",
        "windDirectionAvg",
        "cloudCoverAvg",
        "precipitationProbabilityAvg",
    ]

    # Tomorrow.io expects ISO timestamps; we request the span that covers desired days
    start_time = datetime.combine(today, datetime.min.time(), tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
    end_time = datetime.combine(end, datetime.max.time(), tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")

    payload = {
        "location": f"{float(lat)},{float(lon)}",
        "fields": fields,
        "timesteps": ["1d"],
        "units": "imperial",
        "startTime": start_time,
        "endTime": end_time,
        "timezone": "UTC",
    }

    r = requests.post(
        TOM_URL,
        params={"apikey": key},
        json=payload,
        headers=dict(HEADERS),
        timeout=30,
    )
    r.raise_for_status()
    data = r.json()

    # Normalize
    timelines = (data.get("data") or {}).get("timelines") or []
    if not timelines:
        return {"issued_at": _utc_now_z(), "rows": []}

    intervals = timelines[0].get("intervals") or []
    rows: List[Dict[str, Any]] = []

    for it in intervals:
        start = it.get("startTime")
        vals = it.get("values") or {}
        if not start or not isinstance(vals, dict):
            continue

        d = str(start)[:10]
        if d not in want:
            continue

        try:
            hi = float(vals["temperatureMax"])
            lo = float(vals["temperatureMin"])
        except Exception:
            continue

        extras: Dict[str, Any] = {}
        v = vals.get("humidityAvg")
        if v is not None:
            try:
                extras["humidity_pct"] = float(v)
            except Exception:
                pass

        v = vals.get("windSpeedAvg")
        if v is not None:
            try:
                extras["wind_speed_mph"] = float(v)
            except Exception:
                pass

        v = vals.get("windDirectionAvg")
        if v is not None:
            try:
                extras["wind_dir_deg"] = float(v)
            except Exception:
                pass

        v = vals.get("cloudCoverAvg")
        if v is not None:
            try:
                extras["cloud_cover_pct"] = float(v)
            except Exception:
                pass

        v = vals.get("precipitationProbabilityAvg")
        if v is not None:
            try:
                extras["precip_prob_pct"] = float(v)
            except Exception:
                pass

        rows.append({"target_date": d, "high": hi, "low": lo, "extras": extras})

    return {"issued_at": _utc_now_z(), "rows": rows}
