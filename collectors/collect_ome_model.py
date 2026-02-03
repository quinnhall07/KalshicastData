# collect_ome_model.py
from __future__ import annotations

import math
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import requests

from config import HEADERS


OME_URL = "https://api.open-meteo.com/v1/forecast"

# Strict collector payload shape:
# {
#   "issued_at": "...Z",
#   "daily": [ {"target_date":"YYYY-MM-DD","high":float,"low":float}, ... ],
#   "hourly": [
#      {"valid_time":"YYYY-MM-DDTHH:MM","temperature_f":float|None,"dewpoint_f":float|None,
#       "humidity_pct":float|None,"wind_speed_mph":float|None,"wind_dir_deg":float|None,
#       "cloud_cover_pct":float|None,"precip_prob_pct":float|None},
#      ...
#   ]
# }


def _utc_now_z() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _to_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        v = float(x)
        if math.isfinite(v):
            return v
        return None
    except Exception:
        return None


def fetch_ome_model_forecast(station: dict, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Open-Meteo multi-model collector.

    Expects params to include:
      - model: str (e.g., "gfs_seamless", "ecmwf_ifs04", etc.)
        or caller passes any other Open-Meteo-supported query params.

    Collects:
      - daily highs/lows (temperature_2m_max/min) when available
      - hourly series (temperature, dewpoint, humidity, wind, cloud cover, precip prob)

    Defaults to today..today+3 (3 days ahead). days_ahead can be overridden via params.
    """
    lat = station.get("lat")
    lon = station.get("lon")
    if lat is None or lon is None:
        raise ValueError("Open-Meteo fetch requires station['lat'] and station['lon'].")

    p: Dict[str, Any] = dict(params or {})

    days_ahead = 3
    if p.get("days_ahead") is not None:
        try:
            days_ahead = int(p.pop("days_ahead"))
        except Exception:
            pass
    days_ahead = max(1, min(7, days_ahead))

    start = date.today()
    end = start + timedelta(days=days_ahead)

    # Daily + Hourly in one call.
    q: Dict[str, Any] = {
        "latitude": float(lat),
        "longitude": float(lon),
        "timezone": "UTC",
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "temperature_unit": "fahrenheit",
        "wind_speed_unit": "mph",
        # Daily highs/lows
        "daily": "temperature_2m_max,temperature_2m_min",
        # Hourly extras
        "hourly": ",".join(
            [
                "temperature_2m",
                "dew_point_2m",
                "relative_humidity_2m",
                "wind_speed_10m",
                "wind_direction_10m",
                "cloud_cover",
                "precipitation_probability",
            ]
        ),
    }

    # Merge caller params (e.g., model selection)
    q.update(p)

    r = requests.get(OME_URL, params=q, headers=dict(HEADERS), timeout=25)
    r.raise_for_status()
    data = r.json()

    if data.get("error"):
        raise RuntimeError(f"Open-Meteo error: {data.get('reason') or data.get('message') or data}")

    issued_at = _utc_now_z()

    # -------- Daily --------
    daily_rows: List[Dict[str, Any]] = []
    daily = data.get("daily") or {}
    d_time = daily.get("time") or []
    d_hi = daily.get("temperature_2m_max") or []
    d_lo = daily.get("temperature_2m_min") or []
    if isinstance(d_time, list) and isinstance(d_hi, list) and isinstance(d_lo, list):
        n = min(len(d_time), len(d_hi), len(d_lo))
        for i in range(n):
            td = str(d_time[i])[:10]
            try:
                high = float(d_hi[i])
                low = float(d_lo[i])
            except Exception:
                continue
            daily_rows.append({"target_date": td, "high": high, "low": low})

    # If daily not present, derive from hourly temperature_2m
    if not daily_rows:
        hourly = data.get("hourly") or {}
        h_time = hourly.get("time") or []
        h_temp = hourly.get("temperature_2m") or []
        if isinstance(h_time, list) and isinstance(h_temp, list) and h_time and h_temp:
            by_day: Dict[str, List[float]] = {}
            for t, v in zip(h_time, h_temp):
                td = str(t)[:10]
                fv = _to_float(v)
                if fv is None:
                    continue
                by_day.setdefault(td, []).append(float(fv))
            for td, vals in sorted(by_day.items()):
                if not vals:
                    continue
                daily_rows.append({"target_date": td, "high": max(vals), "low": min(vals)})

    # -------- Hourly --------
    hourly_rows: List[Dict[str, Any]] = []
    hourly = data.get("hourly") or {}
    t = hourly.get("time") or []
    temp = hourly.get("temperature_2m") or []
    dew = hourly.get("dew_point_2m") or []
    rh = hourly.get("relative_humidity_2m") or []
    ws = hourly.get("wind_speed_10m") or []
    wd = hourly.get("wind_direction_10m") or []
    cc = hourly.get("cloud_cover") or []
    pp = hourly.get("precipitation_probability") or []

    if isinstance(t, list) and t:
        n = len(t)

        def _at(arr: Any, i: int) -> Optional[float]:
            if not isinstance(arr, list) or i >= len(arr):
                return None
            return _to_float(arr[i])

        for i in range(n):
            vt = str(t[i])[:16]  # "YYYY-MM-DDTHH:MM"
            hourly_rows.append(
                {
                    "valid_time": vt,
                    "temperature_f": _at(temp, i),
                    "dewpoint_f": _at(dew, i),
                    "humidity_pct": _at(rh, i),
                    "wind_speed_mph": _at(ws, i),
                    "wind_dir_deg": _at(wd, i),
                    "cloud_cover_pct": _at(cc, i),
                    "precip_prob_pct": _at(pp, i),
                }
            )

    return {"issued_at": issued_at, "daily": daily_rows, "hourly": hourly_rows}
