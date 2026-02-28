from __future__ import annotations

import pandas as pd
import requests

from data_extract.session import LONDON_TZ

THAMES_MEASURE = "0006-level-tidal_level-i-15_min-mAOD"


def get_thames(limit: int = 300) -> pd.DataFrame:
    resp = requests.get(
        f"https://environment.data.gov.uk/flood-monitoring/id/measures/{THAMES_MEASURE}/readings",
        params={"_sorted": "", "_limit": limit},
        timeout=20,
    )
    resp.raise_for_status()
    items = resp.json().get("items", [])
    if not items:
        return pd.DataFrame(columns=["time", "level"])
    df = pd.DataFrame(items)[["dateTime", "value"]].rename(
        columns={"dateTime": "time", "value": "level"}
    )
    df["time"] = pd.to_datetime(df["time"], utc=True).dt.tz_convert(LONDON_TZ)
    return df.sort_values("time").reset_index(drop=True)
