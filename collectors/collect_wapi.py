# collect_wapi.py
from __future__ import annotations

import os
from datetime import date, datetime, timezone
from typing import Any, Dict, List

import requests

from config import HEADERS

WAPI_URL = "https://api.weatherapi.com/v1/forecast.json"


def _get_key() -> str:
    key = os.getenv("WEATHERAPI_KEY")
    if not key:
        raise RuntimeError("Missing WEATHERAPI_KEY env var")
    return key


def _utc_now_z() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def fetch_wapi_forecast(station: dict, params: Dict[str, Any] | None = None) -> Dict[str, Any]:
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
      - Extras are daily aggregates from WeatherAPI if available.
    """
    params = params or {}

    lat = station.get("lat")
    lon = station.get("lon")
    if lat is None or lon is None:
        raise ValueError("WeatherAPI fetch requires station['lat'] and station['lon'].")

    key = _get_key()

    ndays = int(params.get("days", 3))
    if ndays < 1:
        ndays = 1

    base = date.today()
    want = {date.fromordinal(base.toordinal() + i).isoformat() for i in range(ndays)}

    q = {
        "key": key,
        "q": f"{float(lat)},{float(lon)}",
        "days": ndays,
        "aqi": "no",
        "alerts": "no",
    }

    r = requests.get(WAPI_URL, params=q, headers=dict(HEADERS), timeout=25)
    r.raise_for_status()
    data = r.json()

    issued_at = _utc_now_z()
    current = data.get("current") or {}
    last_updated = current.get("last_updated")
    if isinstance(last_updated, str) and last_updated.strip():
        # WeatherAPI timestamps are local to location; keep our issued_at as UTC snapshot time
        # to avoid mixing tz semantics across providers.
        pass

    forecast = (data.get("forecast") or {}).get("forecastday") or []
    rows: List[Dict[str, Any]] = []

    for day in forecast:
        d = str(day.get("date") or "")[:10]
        if not d or d not in want:
            continue

        daydata = day.get("day") or {}
        try:
            hi = float(daydata.get("maxtemp_f"))
            lo = float(daydata.get("mintemp_f"))
        except Exception:
            continue

        extras: Dict[str, Any] = {}

        v = daydata.get("avghumidity")
        if v is not None:
            try:
                extras["humidity_pct"] = float(v)
            except Exception:
                pass

        v = daydata.get("maxwind_mph")
        if v is not None:
            try:
                extras["wind_speed_mph"] = float(v)
            except Exception:
                pass

        v = daydata.get("daily_chance_of_rain")
        if v is not None:
            try:
                extras["precip_prob_pct"] = float(v)
            except Exception:
                pass

        v = daydata.get("daily_chance_of_snow")
        # Optional: you can keep this in extras later if you add a column; for now ignore.

        rows.append({"target_date": d, "high": hi, "low": lo, "extras": extras})

    return {"issued_at": issued_at, "rows": rows}
