from __future__ import annotations

import os
import re
from datetime import datetime, time, timedelta
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

from data_extract.session import LONDON_TZ

AERODATABOX_HOST = "aerodatabox.p.rapidapi.com"
AIRPORT = "LHR"
HEATHROW_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}


def _load_dotenv() -> None:
    env_paths = [Path(".env"), Path(__file__).resolve().parents[1] / ".env"]
    for env_path in env_paths:
        if not env_path.exists():
            continue
        for raw in env_path.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip("'\"")
            if key and key not in os.environ:
                os.environ[key] = value
        break


def _rapidapi_headers() -> dict[str, str]:
    _load_dotenv()
    api_key = os.getenv("AERODATABOX_KEY", "").strip()
    if not api_key:
        raise RuntimeError("Missing AERODATABOX_KEY environment variable")
    return {"x-rapidapi-host": AERODATABOX_HOST, "x-rapidapi-key": api_key}


def _fetch_flights_range(from_local: str, to_local: str) -> dict:
    url = f"https://{AERODATABOX_HOST}/flights/airports/iata/{AIRPORT}/{from_local}/{to_local}"
    params = {
        "direction": "Both",
        "withLeg": "true",
        "withCancelled": "true",
        "withCodeshared": "true",
        "withCargo": "true",
        "withPrivate": "true",
        "withLocation": "false",
    }
    resp = requests.get(url, params=params, headers=_rapidapi_headers(), timeout=25)
    resp.raise_for_status()
    payload = resp.json()
    if not isinstance(payload, dict):
        raise RuntimeError(f"Unexpected AeroDataBox payload type: {type(payload).__name__}")
    if "arrivals" not in payload or "departures" not in payload:
        msg = payload.get("message") or payload.get("error") or str(payload)[:240]
        raise RuntimeError(f"Unexpected AeroDataBox payload shape: {msg}")
    return payload


def _extract_local_timestamp(flight: dict, direction: str) -> pd.Timestamp | None:
    if direction == "arrivals":
        path_candidates = [
            ("arrival", "scheduledTime", "local"),
            ("arrival", "revisedTime", "local"),
            ("arrival", "actualTime", "local"),
            ("arrival", "predictedTime", "local"),
        ]
    else:
        path_candidates = [
            ("departure", "scheduledTime", "local"),
            ("departure", "revisedTime", "local"),
            ("departure", "actualTime", "local"),
            ("departure", "predictedTime", "local"),
        ]

    for p0, p1, p2 in path_candidates:
        value = flight.get(p0, {}).get(p1, {}).get(p2)
        if value:
            ts = pd.to_datetime(value, errors="coerce")
            if pd.notna(ts):
                return ts.tz_convert(LONDON_TZ) if ts.tzinfo else ts.tz_localize(LONDON_TZ)
    return None


def _session_flights_aerodatabox(start: datetime, end: datetime) -> tuple[list[pd.Timestamp], list[pd.Timestamp]]:
    def collect(windows: list[tuple[datetime, datetime]]) -> tuple[list[pd.Timestamp], list[pd.Timestamp]]:
        arrivals: list[pd.Timestamp] = []
        departures: list[pd.Timestamp] = []
        for ws, we in windows:
            print(f"[Flights] API window {ws.strftime('%Y-%m-%d %H:%M')} -> {we.strftime('%Y-%m-%d %H:%M')} (London)")
            payload = _fetch_flights_range(ws.strftime("%Y-%m-%dT%H:%M"), we.strftime("%Y-%m-%dT%H:%M"))
            print(
                f"[Flights] API raw window counts: arrivals={len(payload.get('arrivals', []))} "
                f"departures={len(payload.get('departures', []))}"
            )
            for item in payload.get("arrivals", []):
                ts = _extract_local_timestamp(item, "arrivals")
                if ts is not None and start <= ts < end:
                    arrivals.append(ts)
            for item in payload.get("departures", []):
                ts = _extract_local_timestamp(item, "departures")
                if ts is not None and start <= ts < end:
                    departures.append(ts)
        return arrivals, departures

    try:
        arrivals, departures = collect([(start, end)])
    except requests.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else None
        if status != 400:
            raise
        print("[Flights] API 24h window rejected (400), retrying as 2x12h windows")
        mid = start + timedelta(hours=12)
        arrivals, departures = collect([(start, mid), (mid, end)])

    print(f"[Flights] API parsed: arrivals={len(arrivals)} departures={len(departures)} total={len(arrivals)+len(departures)}")
    return arrivals, departures


def _session_flights_heathrow_site(start: datetime, end: datetime) -> tuple[list[pd.Timestamp], list[pd.Timestamp]]:
    with requests.Session() as session:
        session.headers.update(HEATHROW_HEADERS)
        home = session.get("https://www.heathrow.com", timeout=25)
        print(f"[Flights] Heathrow home status={home.status_code} bytes={len(home.text)}")
        arrivals_resp = session.get("https://www.heathrow.com/arrivals", timeout=25)
        departures_resp = session.get("https://www.heathrow.com/departures", timeout=25)
        print(
            f"[Flights] Heathrow pages status: arrivals={arrivals_resp.status_code} "
            f"departures={departures_resp.status_code}"
        )
        print(
            f"[Flights] Heathrow pages bytes: arrivals={len(arrivals_resp.text)} "
            f"departures={len(departures_resp.text)}"
        )
        arrivals_html = arrivals_resp.text
        departures_html = departures_resp.text
    arrivals = _extract_times_from_heathrow_html(arrivals_html, start, end)
    departures = _extract_times_from_heathrow_html(departures_html, start, end)
    print(
        f"[Flights] Heathrow parsed: arrivals={len(arrivals)} departures={len(departures)} "
        f"total={len(arrivals)+len(departures)}"
    )
    return arrivals, departures


def _extract_times_from_heathrow_html(html: str, start: datetime, end: datetime) -> list[pd.Timestamp]:
    soup = BeautifulSoup(html, "html.parser")
    out: list[pd.Timestamp] = []

    for tag in soup.find_all(attrs={"datetime": True}):
        ts = _parse_any_time(str(tag.get("datetime")), start, end)
        if ts is not None:
            out.append(ts)

    for tag in soup.find_all(True):
        for attr, value in tag.attrs.items():
            if "time" not in str(attr).lower():
                continue
            if isinstance(value, list):
                value = " ".join(map(str, value))
            ts = _parse_any_time(str(value), start, end)
            if ts is not None:
                out.append(ts)

    if not out:
        text = soup.get_text(" ", strip=True)
        for hhmm in set(re.findall(r"\b(?:[01]\d|2[0-3]):[0-5]\d\b", text)):
            ts = _parse_any_time(hhmm, start, end)
            if ts is not None:
                out.append(ts)

    uniq = sorted(set(out))
    return [ts for ts in uniq if start <= ts < end]


def _parse_any_time(raw: str, start: datetime, end: datetime) -> pd.Timestamp | None:
    value = (raw or "").strip()
    if not value:
        return None

    parsed = pd.to_datetime(value, errors="coerce")
    if pd.notna(parsed):
        ts = parsed.tz_convert(LONDON_TZ) if parsed.tzinfo else parsed.tz_localize(LONDON_TZ)
        return ts if start <= ts < end else None

    m = re.search(r"\b([01]\d|2[0-3]):([0-5]\d)\b", value)
    if not m:
        return None
    hh = int(m.group(1))
    mm = int(m.group(2))
    base_date = start.date() if hh >= 12 else end.date()
    ts = pd.Timestamp(datetime.combine(base_date, time(hh, mm), tzinfo=LONDON_TZ))
    return ts if start <= ts < end else None


def get_session_flights(start: datetime, end: datetime) -> tuple[list[pd.Timestamp], list[pd.Timestamp]]:
    use_api_primary = os.getenv("USE_AERODATABOX_PRIMARY", "1").strip() == "1"
    if use_api_primary:
        try:
            arrivals, departures = _session_flights_aerodatabox(start, end)
            if not arrivals and not departures:
                raise RuntimeError("AeroDataBox returned zero flights for settlement window")
            print("[Flights] source=API")
            return arrivals, departures
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            if status == 429:
                raise RuntimeError("AeroDataBox rate-limited (429)") from exc
            print(f"[Flights] API failed, falling back to Heathrow HTML: {exc}")
            return _session_flights_heathrow_site(start, end)
        except Exception as exc:
            print(f"[Flights] API failed, falling back to Heathrow HTML: {exc}")
            return _session_flights_heathrow_site(start, end)

    try:
        arrivals, departures = _session_flights_heathrow_site(start, end)
        if arrivals or departures:
            print(f"[Flights] source=Heathrow HTML arrivals={len(arrivals)} departures={len(departures)} total={len(arrivals)+len(departures)}")
            return arrivals, departures
    except Exception:
        pass
    arrivals, departures = _session_flights_aerodatabox(start, end)
    print("[Flights] source=API (fallback)")
    return arrivals, departures
