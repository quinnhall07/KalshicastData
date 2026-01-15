# collect_ome.py
from __future__ import annotations

import requests
from datetime import date, datetime, timezone
from typing import Any, Dict, List

from config import HEADERS

OME_URL = "https://api.open-meteo.com/v1/forecast"


def fetch_ome_forecast(station: dict, params: Dict[str, Any] | None = None) -> Dict[str, Any]:
    lat = station.get("lat")
    lon = station.get("lon")
    if lat is None or lon is None:
        raise ValueError("Open-Meteo fetch requires station['lat'] and station['lon'].")

    today = date.today()
    tomorrow = date.fromordinal(today.toordinal() + 1)
    want = {today.isoformat(), tomorrow.isoformat()}

    q: Dict[str, Any] = {
        "latitude": float(lat),
        "longitude": float(lon),
        "daily": "temperature_2m_max,temperature_2m_min",
        "timezone": "UTC",
        "start_date": today.isoformat(),
        "end_date": tomorrow.isoformat(),
        "temperature_unit": "fahrenheit",
    }

    r = requests.get(OME_URL, params=q, headers=dict(HEADERS), timeout=20)
    r.raise_for_status()
    data = r.json()

    if data.get("error"):
        raise RuntimeError(f"Open-Meteo error: {data.get('reason') or data.get('message') or data}")

    daily = data.get("daily") or {}
    dates = daily.get("time") or []
    tmax = daily.get("temperature_2m_max") or []
    tmin = daily.get("temperature_2m_min") or []
    if not dates:
        return {"issued_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"), "rows": []}

    n = min(len(dates), len(tmax), len(tmin))
    rows: List[dict] = []
    for i in range(n):
        d = str(dates[i])[:10]
        if d not in want:
            continue
        try:
            rows.append({"target_date": d, "high": float(tmax[i]), "low": float(tmin[i])})
        except Exception:
            continue

    issued_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return {"issued_at": issued_at, "rows": rows}
