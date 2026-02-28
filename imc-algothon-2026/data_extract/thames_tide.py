from __future__ import annotations

import json
import time
from pathlib import Path

import pandas as pd
import requests
from requests import RequestException

from data_extract.session import LONDON_TZ, settlement_session_bounds

THAMES_MEASURE = "0006-level-tidal_level-i-15_min-mAOD"


def _to_dataframe(items: list[dict]) -> pd.DataFrame:
    if not items:
        return pd.DataFrame(columns=["time", "level"])
    df = pd.DataFrame(items)[["dateTime", "value"]].rename(
        columns={"dateTime": "time", "value": "level"}
    )
    df["time"] = pd.to_datetime(df["time"], utc=True).dt.tz_convert(LONDON_TZ)
    return df.sort_values("time").reset_index(drop=True)


def _load_cached(cache_file: str) -> pd.DataFrame:
    path = Path(cache_file)
    if not path.exists():
        raise RuntimeError("Thames API unavailable and no cached tide file found")
    df = pd.read_csv(path)
    if df.empty or "time" not in df.columns or "level" not in df.columns:
        raise RuntimeError("Cached tide file is missing required columns: time, level")
    parsed = pd.to_datetime(df["time"], errors="coerce")
    if parsed.isna().all():
        raise RuntimeError("Cached tide file contains invalid time values")
    if getattr(parsed.dt, "tz", None) is None:
        df["time"] = parsed.dt.tz_localize(LONDON_TZ)
    else:
        df["time"] = parsed.dt.tz_convert(LONDON_TZ)
    df["level"] = pd.to_numeric(df["level"], errors="coerce")
    df = df.dropna(subset=["time", "level"])
    if df.empty:
        raise RuntimeError("Cached tide file has no valid rows after parsing")
    return df.sort_values("time").reset_index(drop=True)


def get_thames(limit: int = 300, retries: int = 3, cache_file: str = "thames_raw_latest.csv") -> pd.DataFrame:
    _ = cache_file  # kept for backward-compatible signature; cache fallback intentionally disabled
    last_exc: Exception | None = None
    for attempt in range(retries):
        try:
            resp = requests.get(
                f"https://environment.data.gov.uk/flood-monitoring/id/measures/{THAMES_MEASURE}/readings",
                params={"_sorted": "", "_limit": limit},
                timeout=(10, 20),
            )
            resp.raise_for_status()
            items = resp.json().get("items", [])
            return _to_dataframe(items)
        except RequestException as exc:
            last_exc = exc
            if attempt < retries - 1:
                time.sleep(1.5 * (attempt + 1))
            continue
    if last_exc is not None:
        raise RuntimeError("Failed to fetch live Thames data after retries") from last_exc
    raise RuntimeError("Failed to fetch live Thames data")


def get_thames_fair_price(
    write_output: bool = False,
    output_dir: str = ".",
) -> tuple[int, int]:
    start, end = settlement_session_bounds()
    df = get_thames()
    session = df[(df["time"] >= start) & (df["time"] < end)].copy()
    if session.empty:
        raise RuntimeError("No Thames readings in current session window")

    # TIDE_SPOT: absolute tidal level at settlement (12pm) in mm AOD.
    settle_idx = (df["time"] - end).abs().idxmin()
    settle_level_m = float(df.loc[settle_idx, "level"])
    tide_spot = abs(settle_level_m) * 1000.0

    # TIDE_SWING: sum over 15m diffs in cm of strangle payoff with strikes 20/25.
    diffs_cm = session["level"].diff().abs().dropna() * 100.0
    put_leg = (20.0 - diffs_cm).clip(lower=0.0)
    call_leg = (diffs_cm - 25.0).clip(lower=0.0)
    tide_swing = (put_leg + call_leg).sum()

    tide_spot_rounded = round(tide_spot)
    tide_swing_rounded = round(tide_swing)

    if write_output:
        out_dir = Path(output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        # Full raw series and session slice for quick debugging / audit.
        df.to_csv(out_dir / "thames_raw_latest.csv", index=False)
        session.to_csv(out_dir / "thames_session_latest.csv", index=False)
        payload = {
            "session_start": start.isoformat(),
            "session_end": end.isoformat(),
            "settlement_level_mAOD": settle_level_m,
            "tide_spot_mm": tide_spot,
            "tide_spot_rounded": tide_spot_rounded,
            "num_session_readings": int(len(session)),
            "num_swing_diffs": int(len(diffs_cm)),
            "tide_swing": float(tide_swing),
            "tide_swing_rounded": tide_swing_rounded,
        }
        with (out_dir / "thames_fair_latest.json").open("w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

    return tide_spot_rounded, tide_swing_rounded
