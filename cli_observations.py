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


# ----------------------------
# Helpers
# ----------------------------

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


def _get_json_with_retry(url: str, *, headers: dict, params: Optional[dict] = None, timeout: int = 25, attempts: int = 3) -> dict:
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


def _get_text_with_retry(url: str, *, headers: dict, timeout: int = 25, attempts: int = 3) -> str:
    last: Optional[Exception] = None
    for i in range(attempts):
        try:
            r = requests.get(url, headers=headers, timeout=timeout)
            r.raise_for_status()
            return r.text
        except Exception as e:
            last = e
            if i == attempts - 1 or not _is_retryable_http(e):
                raise
            time_mod.sleep(1.25 * (i + 1))
    raise last  # pragma: no cover


def _utc_iso_from_any(s: Optional[str]) -> Optional[str]:
    if not s or not isinstance(s, str):
        return None
    # NWS typically returns ISO timestamps already; keep as-is
    return s


def _station_to_climate_site(station_id: str) -> str:
    # Common case: ICAO KXXX => climate site XXX
    if station_id and station_id.startswith("K") and len(station_id) == 4:
        return station_id[1:]
    return station_id


def _get_cwa_for_station(lat: float, lon: float) -> str:
    # NWS points endpoint returns properties.cwa (forecast office / CWA)
    url = f"https://api.weather.gov/points/{lat:.4f},{lon:.4f}"
    headers = dict(HEADERS)
    headers["Accept"] = "application/geo+json"
    payload = _get_json_with_retry(url, headers=headers, timeout=20, attempts=3)
    props = payload.get("properties") or {}
    cwa = props.get("cwa")
    if not cwa:
        raise ValueError("NWS points endpoint missing properties.cwa")
    return str(cwa).strip().upper()


def _list_cli_products_for_cwa(cwa: str, *, limit: int = 25) -> List[dict]:
    # List CLI products issued by this office
    # Endpoint exists per NWS API spec (products by type + location). :contentReference[oaicite:2]{index=2}
    url = f"https://api.weather.gov/products/types/CLI/locations/{cwa}"
    headers = dict(HEADERS)
    headers["Accept"] = "application/ld+json"
    payload = _get_json_with_retry(url, headers=headers, params={"limit": limit}, timeout=25, attempts=3)

    # NWS JSON-LD collections commonly expose items under @graph
    items = payload.get("@graph")
    if isinstance(items, list):
        return [x for x in items if isinstance(x, dict)]
    # Fallback
    items = payload.get("graph") or payload.get("items")
    if isinstance(items, list):
        return [x for x in items if isinstance(x, dict)]
    return []


def _fetch_product_text_and_issued_at(product_id_or_url: str) -> Tuple[Optional[str], Optional[str]]:
    # product_id_or_url may be full URL or ID; normalize to URL
    if product_id_or_url.startswith("http"):
        url = product_id_or_url
    else:
        url = f"https://api.weather.gov/products/{product_id_or_url}"

    headers = dict(HEADERS)
    headers["Accept"] = "application/ld+json"
    payload = _get_json_with_retry(url, headers=headers, timeout=25, attempts=3)

    issued_at = _utc_iso_from_any(payload.get("issuanceTime") or payload.get("issueTime") or payload.get("issuedAt"))
    text = payload.get("productText") or payload.get("text") or payload.get("product_text")
    if not isinstance(text, str):
        text = None
    return text, issued_at


def _parse_cli_max_min_f(cli_text: str) -> Optional[Tuple[float, float]]:
    """
    CLI (Daily Climate Report) formats vary a bit by office.
    Try several patterns that show up commonly.
    """
    t = cli_text

    patterns_max = [
        r"MAXIMUM(?:\s+TEMPERATURE)?\s*[:\-]\s*([\-]?\d+(?:\.\d+)?)",
        r"MAX(?:IMUM)?\s+TEMP(?:ERATURE)?\s*[:\-]\s*([\-]?\d+(?:\.\d+)?)",
        r"HIGH(?:\s+TEMPERATURE)?\s*[:\-]\s*([\-]?\d+(?:\.\d+)?)",
    ]
    patterns_min = [
        r"MINIMUM(?:\s+TEMPERATURE)?\s*[:\-]\s*([\-]?\d+(?:\.\d+)?)",
        r"MIN(?:IMUM)?\s+TEMP(?:ERATURE)?\s*[:\-]\s*([\-]?\d+(?:\.\d+)?)",
        r"LOW(?:\s+TEMPERATURE)?\s*[:\-]\s*([\-]?\d+(?:\.\d+)?)",
    ]

    max_v: Optional[float] = None
    min_v: Optional[float] = None

    for p in patterns_max:
        m = re.search(p, t, flags=re.IGNORECASE)
        if m:
            try:
                max_v = float(m.group(1))
                break
            except ValueError:
                pass

    for p in patterns_min:
        m = re.search(p, t, flags=re.IGNORECASE)
        if m:
            try:
                min_v = float(m.group(1))
                break
            except ValueError:
                pass

    if max_v is None or min_v is None:
        return None

    # If the report is in whole °F already, keep. If you ever see °C values here, add conversion.
    return round(max_v, 1), round(min_v, 1)


def _find_best_cli_for_target_date(items: List[dict], target_date: str) -> Optional[str]:
    """
    Pick a likely candidate product id/url.
    Minimal strategy: take newest first (API is typically newest-first),
    and try until we find one that parses.
    """
    for it in items:
        pid = it.get("id") or it.get("@id") or it.get("productId")
        if isinstance(pid, str) and pid.strip():
            return pid.strip()
    return None


# ----------------------------
# Main entry points
# ----------------------------

def fetch_observations_for_station(station: dict, target_date: str) -> bool:
    station_id = station["station_id"]

    # Keep locations table up to date (idempotent)
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

    # --- Option A: CLI text product (official daily climate report) ---
    try:
        if lat is None or lon is None:
            raise ValueError("station missing lat/lon (needed to derive CWA for CLI lookup)")

        cwa = _get_cwa_for_station(float(lat), float(lon))
        items = _list_cli_products_for_cwa(cwa, limit=25)
        pid = _find_best_cli_for_target_date(items, target_date)
        if not pid:
            raise ValueError(f"no CLI products returned for CWA={cwa}")

        # Try a few products until one parses
        for _ in range(6):
            text, issued_at = _fetch_product_text_and_issued_at(pid)
            if text:
                parsed = _parse_cli_max_min_f(text)
                if parsed:
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

            # If it didn't parse, try next product in the listing (if present)
            # (very simple: move to next item)
            idx = next((i for i, it in enumerate(items) if (it.get("id") or it.get("@id")) == pid), None)
            if idx is not None and idx + 1 < len(items):
                pid = (items[idx + 1].get("id") or items[idx + 1].get("@id") or "").strip()
                if not pid:
                    break
            else:
                break

        raise ValueError("CLI fetched but could not parse max/min from productText")

    except Exception as e:
        # Optional fallback: keep data continuity if CLI fails
        # You can delete this entire block if you want CLI-only strictness.
        try:
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

            payload = _get_json_with_retry(url, headers=headers, params=params, timeout=25, attempts=3)
            feats = payload.get("features", [])
            temps_f = _extract_temps_f(feats)

            if not temps_f:
                print(f"[obs] FAIL {station_id} {target_date}: CLI failed ({e}); obs fallback: no temps")
                return False

            high = round(max(temps_f), 1)
            low = round(min(temps_f), 1)

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

        except Exception as e2:
            print(f"[obs] FAIL {station_id} {target_date}: CLI failed ({e}); fallback failed ({e2})")
            return False


def fetch_observations(target_date: str) -> bool:
    any_ok = False
    for st in STATIONS:
        try:
            ok = fetch_observations_for_station(st, target_date)
            any_ok = any_ok or ok
        except Exception as e:
            print(f"[obs] FAIL {st.get('station_id')} {target_date}: {e}")
    return any_ok
