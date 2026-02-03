# collect_vcr.py
from __future__ import annotations

import os
from datetime import date, datetime, timezone
from typing import Any, Dict, List

import requests

from config import HEADERS

VCR_URL = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline"


def _utc_now_z() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _get_key() -> str:
    key = os.getenv("VISUALCROSSING_KEY")
    if not key:
        raise RuntimeError("Missing VISUALCROSSING_KEY env var")
    return key


def fetch_vcr_forecast(station: dict, params: Dict[str, Any] | None = None) -> Dict[str, Any]:
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
      - Extras are daily aggregates from Visual Crossing if available.
    """
    params = params or {}

    lat = station.get("lat")
    lon = station.get("lon")
    if lat is None or lon is None:
        raise ValueError("Visual Crossing fetch requires station['lat'] and station['lon'].")

    key = _get_key()

    ndays = int(params.get("days", 3))
    if ndays < 1:
        ndays = 1

    base = date.today()
    start = base.isoformat()
    end = date.fromordinal(base.toordinal() + (ndays - 1)).isoformat()
    want = {date.fromordinal(base.toordinal() + i).isoformat() for i in range(ndays)}

    # unitGroup=us returns Fahrenheit, mph, etc.
    q = {
        "unitGroup": "us",
        "key": key,
        "contentType": "json",
        "include": "days",
    }

    url = f"{VCR_URL}/{float(lat)},{float(lon)}/{start}/{end}"
    r = requests.get(url, params=q, headers=dict(HEADERS), timeout=25)
    r.raise_for_status()
    data = r.json()

    days = data.get("days") or []
    rows: List[Dict[str, Any]] = []

    for drec in days:
        d = str(drec.get("datetime") or "")[:10]
        if not d or d not in want:
            continue

        try:
            hi = float(drec.get("tempmax"))
            lo = float(drec.get("tempmin"))
        except Exception:
            continue

        extras: Dict[str, Any] = {}

        v = drec.get("humidity")
        if v is not None:
            try:
                extras["humidity_pct"] = float(v)
            except Exception:
                pass

        v = drec.get("windspeed")
        if v is not None:
            try:
                extras["wind_speed_mph"] = float(v)
            except Exception:
                pass

        v = drec.get("winddir")
        if v is not None:
            try:
                extras["wind_dir_deg"] = float(v)
            except Exception:
                pass

        v = drec.get("cloudcover")
        if v is not None:
            try:
                extras["cloud_cover_pct"] = float(v)
            except Exception:
                pass

        # Visual Crossing provides precipprob in some plans/fields; keep if present.
        v = drec.get("precipprob")
        if v is not None:
            try:
                extras["precip_prob_pct"] = float(v)
            except Exception:
                pass

        rows.append({"target_date": d, "high": hi, "low": lo, "extras": extras})

    return {"issued_at": _utc_now_z(), "rows": rows}
