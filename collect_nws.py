# collect_nws.py
from __future__ import annotations

import requests
from datetime import date, datetime, timezone
from typing import Dict, List, Tuple

from config import HEADERS


def _station_latlon(station_id: str) -> Tuple[float, float]:
    url = f"https://api.weather.gov/stations/{station_id}"
    headers = dict(HEADERS)
    headers["Accept"] = "application/geo+json"

    r = requests.get(url, headers=headers, timeout=20)
    r.raise_for_status()
    data = r.json()

    coords = data.get("geometry", {}).get("coordinates")
    if not coords or len(coords) != 2:
        raise RuntimeError(f"NWS station {station_id} missing geometry coordinates")
    lon, lat = coords
    return float(lat), float(lon)


def _forecast_url_from_latlon(lat: float, lon: float) -> str:
    url = f"https://api.weather.gov/points/{lat},{lon}"
    headers = dict(HEADERS)
    headers["Accept"] = "application/geo+json"

    r = requests.get(url, headers=headers, timeout=20)
    r.raise_for_status()
    props = r.json().get("properties", {})
    forecast_url = props.get("forecast")
    if not forecast_url:
        raise RuntimeError(f"NWS points response missing forecast URL for {lat},{lon}")
    return forecast_url


def _summarize_high_low_by_date(periods: List[dict]) -> Dict[str, Tuple[float, float]]:
    out: Dict[str, List[float]] = {}
    for p in periods:
        start = p.get("startTime")
        temp = p.get("temperature")
        if start is None or temp is None:
            continue
        d = str(start)[:10]
        out.setdefault(d, []).append(float(temp))

    summarized: Dict[str, Tuple[float, float]] = {}
    for d, temps in out.items():
        summarized[d] = (max(temps), min(temps))
    return summarized


def fetch_nws_forecast(station: dict) -> Dict[str, Any]:
    station_id = station.get("station_id")

    lat = station.get("lat")
    lon = station.get("lon")

    if lat is None or lon is None:
        if not station_id:
            raise ValueError("Need either (lat, lon) or station_id for NWS fetch")
        lat, lon = _station_latlon(station_id)

    forecast_url = _forecast_url_from_latlon(float(lat), float(lon))

    headers = dict(HEADERS)
    headers["Accept"] = "application/geo+json"

    r = requests.get(forecast_url, headers=headers, timeout=20)
    r.raise_for_status()
    data = r.json()

    periods = data.get("properties", {}).get("periods", [])
    if not periods:
        raise RuntimeError(f"NWS forecast returned no periods for station {station_id}")

    hl_map = _summarize_high_low_by_date(periods)

    today = date.today()
    tomorrow = date.fromordinal(today.toordinal() + 1)
    want = [today.isoformat(), tomorrow.isoformat()]

    rows: List[dict] = []
    for d in want:
        if d in hl_map:
            high, low = hl_map[d]
            rows.append({"target_date": d, "high": float(high), "low": float(low)})

    issued_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return {"issued_at": issued_at, "rows": rows}
