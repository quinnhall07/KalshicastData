# cli_observations.py
from __future__ import annotations

import math
import re
import time as time_mod
import requests
from datetime import datetime, date, time
from typing import Optional, List, Tuple

from zoneinfo import ZoneInfo

from config import STATIONS, HEADERS
from db import upsert_observation, upsert_location


def c_to_f(c: float) -> float:
    return (c * 9.0 / 5.0) + 32.0


def _extract_temps_f(features: List[dict]) -> List[float]:
    temps: List[float] = []
    for feat in features:
        v = feat.get("properties", {}).get("temperature", {}).get("value")
        if v is None:
            continue
        try:
            f = c_to_f(float(v))
            if math.isfinite(f):
                temps.append(f)
        except (TypeError, ValueError):
            continue
    return temps


def _is_retryable_http(e: Exception) -> bool:
    if isinstance(e, (requests.Timeout, requests.ConnectionError)):
        return True
    if isinstance(e, requests.HTTPError):
        resp = getattr(e, "response", None)
        code = getattr(resp, "status_code", None)
        return code is None or code == 429 or code >= 500
    return False


def _get_json(url: str, *, headers: dict, params: Optional[dict] = None, timeout: int = 25, attempts: int = 3) -> dict:
    last: Optional[Exception] = None
    for i in range(attempts):
        try:
            r = requests.get(url, headers=headers, params=params, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last = e
            if i == attempts - 1 or not _is_retryable_http(e):
                raise
            time_mod.sleep(1.25 * (i + 1))
    raise last  # pragma: no cover


def _get_cwa(lat: float, lon: float) -> str:
    url = f"https://api.weather.gov/points/{lat:.4f},{lon:.4f}"
    headers = dict(HEADERS)
    headers["Accept"] = "application/geo+json"
    payload = _get_json(url, headers=headers, timeout=20, attempts=3)
    props = payload.get("properties") or {}
    cwa = props.get("cwa")
    if not cwa:
        raise ValueError("NWS points endpoint missing properties.cwa")
    return str(cwa).strip().upper()


def _fetch_product(product_id_or_url: str) -> Tuple[str, Optional[str]]:
    url = product_id_or_url if product_id_or_url.startswith("http") else f"https://api.weather.gov/products/{product_id_or_url}"
    headers = dict(HEADERS)
    headers["Accept"] = "application/ld+json"
    payload = _get_json(url, headers=headers, timeout=25, attempts=3)

    text = payload.get("productText") or payload.get("text")
    if not isinstance(text, str) or not text.strip():
        raise ValueError("product missing productText")

    issued_at = payload.get("issuanceTime") or payload.get("issueTime") or payload.get("issuedAt")
    return text, issued_at if isinstance(issued_at, str) else None


def _parse_cli_max_min(text: str) -> Optional[Tuple[float, float]]:
    # 1) Try explicit "MAXIMUM: 27" style (some offices)
    max_patterns = [
        r"\bMAXIMUM(?:\s+TEMPERATURE)?\s*[:\-]\s*([\-]?\d+(?:\.\d+)?)\b",
        r"\bMAX(?:IMUM)?\s+TEMP(?:ERATURE)?\s*[:\-]\s*([\-]?\d+(?:\.\d+)?)\b",
        r"\bHIGH(?:\s+TEMPERATURE)?\s*[:\-]\s*([\-]?\d+(?:\.\d+)?)\b",
        # dot-leader variants: MAXIMUM .... 27
        r"\bMAXIMUM(?:\s+TEMPERATURE)?(?:\s*\(.*?\))?\s*\.{2,}\s*([\-]?\d+(?:\.\d+)?)\b",
    ]
    min_patterns = [
        r"\bMINIMUM(?:\s+TEMPERATURE)?\s*[:\-]\s*([\-]?\d+(?:\.\d+)?)\b",
        r"\bMIN(?:IMUM)?\s+TEMP(?:ERATURE)?\s*[:\-]\s*([\-]?\d+(?:\.\d+)?)\b",
        r"\bLOW(?:\s+TEMPERATURE)?\s*[:\-]\s*([\-]?\d+(?:\.\d+)?)\b",
        # dot-leader variants: MINIMUM .... 12
        r"\bMINIMUM(?:\s+TEMPERATURE)?(?:\s*\(.*?\))?\s*\.{2,}\s*([\-]?\d+(?:\.\d+)?)\b",
    ]

    hi: Optional[float] = None
    lo: Optional[float] = None

    for p in max_patterns:
        m = re.search(p, text, flags=re.IGNORECASE)
        if m:
            try:
                hi = float(m.group(1))
                break
            except ValueError:
                pass

    for p in min_patterns:
        m = re.search(p, text, flags=re.IGNORECASE)
        if m:
            try:
                lo = float(m.group(1))
                break
            except ValueError:
                pass

    # 2) Table format (your MDW sample)
    if hi is None:
        m = re.search(r"^\s*MAXIMUM\s+([\-]?\d+(?:\.\d+)?)\b", text, flags=re.IGNORECASE | re.MULTILINE)
        if m:
            hi = float(m.group(1))

    if lo is None:
        m = re.search(r"^\s*MINIMUM\s+([\-]?\d+(?:\.\d+)?)\b", text, flags=re.IGNORECASE | re.MULTILINE)
        if m:
            lo = float(m.group(1))

    if hi is None or lo is None:
        return None
    return round(hi, 1), round(lo, 1)

def _cli_matches_site(text: str, cli_site: str) -> bool:
    t = text.upper()
    s = cli_site.upper()
    return (f"CLI{s}" in t) or (re.search(rf"\bCLI{s}\b", t) is not None) or (s in t)

def _parse_cli_report_date(text: str) -> Optional[str]:
    # Examples:
    # "...THE CHICAGO-MIDWAY CLIMATE SUMMARY FOR JANUARY 31 2026..."
    m = re.search(
        r"\bCLIMATE\s+SUMMARY\s+FOR\s+([A-Z]+\s+\d{1,2}\s+\d{4})\b",
        text.upper(),
        flags=re.DOTALL,
    )
    if not m:
        return None
    try:
        dt = datetime.strptime(m.group(1).title(), "%B %d %Y").date()
        return dt.isoformat()
    except Exception:
        return None


def _fallback_station_obs(station: dict, target_date: str) -> Optional[Tuple[float, float]]:
    station_id = station["station_id"]
    target = date.fromisoformat(target_date)
    tz = ZoneInfo(station.get("timezone") or "UTC")

    start_local = datetime.combine(target, time(0, 0), tzinfo=tz)
    end_local = datetime.combine(target, time(23, 59), tzinfo=tz)
    start_utc = start_local.astimezone(ZoneInfo("UTC")).isoformat()
    end_utc = end_local.astimezone(ZoneInfo("UTC")).isoformat()

    url = f"https://api.weather.gov/stations/{station_id}/observations"
    params = {"start": start_utc, "end": end_utc, "limit": 500}
    headers = dict(HEADERS)
    headers["Accept"] = "application/geo+json"

    payload = _get_json(url, headers=headers, params=params, timeout=25, attempts=3)
    feats = payload.get("features", [])
    temps_f = _extract_temps_f(feats)
    if not temps_f:
        return None
    return round(max(temps_f), 1), round(min(temps_f), 1)

def _list_cli_products(cwa: str) -> List[dict]:
    url = f"https://api.weather.gov/products/types/CLI/locations/{cwa}"
    headers = dict(HEADERS)
    headers["Accept"] = "application/ld+json"
    payload = _get_json(url, headers=headers, params=None, timeout=25, attempts=3)
    items = payload.get("@graph")
    return [x for x in items if isinstance(x, dict)] if isinstance(items, list) else []

def _fetch_cli_text_with_fallback(cwa: str) -> tuple[str, Optional[str]]:
    try:
        return _fetch_latest_cli_text(cwa)
    except Exception:
        pass

    items = _list_cli_products(cwa)
    if not items:
        raise ValueError(f"no CLI products for CWA={cwa}")

    # sort newest-first if issuance timestamp exists
    def _ts(it: dict) -> str:
        return str(it.get("issuanceTime") or it.get("issueTime") or it.get("issuedAt") or "")

    items_sorted = sorted(items, key=_ts, reverse=True)

    last_err: Optional[Exception] = None
    for it in items_sorted[:30]:
        pid = it.get("id") or it.get("@id")
        if not isinstance(pid, str) or not pid.strip():
            continue
        try:
            return _fetch_product(pid.strip())
        except Exception as e:
            last_err = e
            continue

    raise ValueError(f"failed to fetch any CLI product text; last_err={last_err}")

def _fetch_latest_cli_text(cwa: str) -> tuple[str, Optional[str]]:
    url = f"https://api.weather.gov/products/types/CLI/locations/{cwa}/latest"
    headers = dict(HEADERS)
    headers["Accept"] = "application/ld+json"
    payload = _get_json(url, headers=headers, timeout=25, attempts=3)
    text = payload.get("productText") or payload.get("text")
    issued_at = payload.get("issuanceTime") or payload.get("issueTime") or payload.get("issuedAt")
    if not isinstance(text, str) or not text.strip():
        raise ValueError("latest CLI missing productText")
    return text, issued_at if isinstance(issued_at, str) else None


def fetch_observations_for_station(station: dict, target_date: str) -> bool:
    station_id = station["station_id"]

    upsert_location({
        "station_id": station_id,
        "name": station.get("name"),
        "lat": station.get("lat"),
        "lon": station.get("lon"),
        "timezone": station.get("timezone"),
        "state": station.get("state"),
        "elevation_ft": station.get("elevation_ft"),
        "is_active": station.get("is_active"),
    })

    lat = station.get("lat")
    lon = station.get("lon")
    cli_site = (station.get("cli_site") or (station_id[1:] if station_id.startswith("K") and len(station_id) == 4 else station_id)).upper()

    # CLI path
    try:
        if lat is None or lon is None:
            raise ValueError("missing lat/lon (required for CLI lookup)")

        cwa = _get_cwa(float(lat), float(lon))
        text, issued_at = _fetch_cli_text_with_fallback(cwa)
        
        # Filter to correct climate site (keeps you from ingesting the wrong city)
        if not _cli_matches_site(text, cli_site):
            raise ValueError(f"latest CLI not for site={cli_site}")

        
        report_date = _parse_cli_report_date(text)
        if report_date and report_date != target_date:
            raise ValueError(f"latest CLI date {report_date} != target {target_date}")
        
        parsed = _parse_cli_max_min(text)
        if not parsed:
            raise ValueError("latest CLI missing max/min")
        
        high, low = parsed
        upsert_observation(
            station_id=station_id,
            obs_date=target_date,
            observed_high=high,
            observed_low=low,
            issued_at=issued_at,
            raw_text=text,
            source="NWS_CLI",
        )
        print(f"[obs] OK {station_id} {target_date}: high={high} low={low} (CLI)")
        return True

    except Exception as e:
        # Fallback (recommended, because CLI is not guaranteed for every site/day)
        fb = _fallback_station_obs(station, target_date)
        if not fb:
            print(f"[obs] FAIL {station_id} {target_date}: CLI failed ({e}); fallback failed")
            return False

        high, low = fb
        upsert_observation(
            station_id=station_id,
            obs_date=target_date,
            observed_high=high,
            observed_low=low,
            issued_at=None,
            raw_text=None,
            source="NWS_OBS_FALLBACK",
        )
        print(f"[obs] OK {station_id} {target_date}: high={high} low={low} (fallback; CLI failed: {e})")
        return True


def fetch_observations(target_date: str) -> bool:
    any_ok = False
    for st in STATIONS:
        try:
            ok = fetch_observations_for_station(st, target_date)
            any_ok = any_ok or ok
        except Exception as e:
            print(f"[obs] FAIL {st.get('station_id')} {target_date}: {e}")
    return any_ok


