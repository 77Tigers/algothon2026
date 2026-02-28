from __future__ import annotations

import pandas as pd
import requests

from data_extract.session import LONDON_TZ

LONDON_LAT = 51.5074
LONDON_LON = -0.1278


def c_to_f(celsius: float) -> float:
    return celsius * 9 / 5 + 32


def get_weather_df(past_steps: int = 96, forecast_steps: int = 96) -> pd.DataFrame:
    variables = (
        "temperature_2m,apparent_temperature,relative_humidity_2m,"
        "precipitation,wind_speed_10m,cloud_cover,visibility"
    )
    resp = requests.get(
        "https://api.open-meteo.com/v1/forecast",
        params={
            "latitude": LONDON_LAT,
            "longitude": LONDON_LON,
            "minutely_15": variables,
            "past_minutely_15": past_steps,
            "forecast_minutely_15": forecast_steps,
            "timezone": "Europe/London",
        },
        timeout=20,
    )
    resp.raise_for_status()
    payload = resp.json()["minutely_15"]
    time_index = pd.to_datetime(payload["time"], errors="coerce")
    if getattr(time_index, "tz", None) is None:
        time_index = time_index.tz_localize(LONDON_TZ)
    else:
        time_index = time_index.tz_convert(LONDON_TZ)
    return pd.DataFrame(
        {
            "time": time_index,
            "temperature_c": payload["temperature_2m"],
            "humidity": payload["relative_humidity_2m"],
        }
    )
