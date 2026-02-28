from __future__ import annotations

import pandas as pd
from data_extract.london_weather import c_to_f, get_weather_df
from data_extract.session import settlement_session_bounds


def fair_price() -> tuple[int, int]:
    start, end = settlement_session_bounds()
    weather = get_weather_df()
    session = weather[(weather["time"] >= start) & (weather["time"] < end)].copy()
    if session.empty:
        raise RuntimeError("No London weather points in current session window")

    session["temp_f"] = session["temperature_c"].map(c_to_f)
    session["wx_prod"] = session["temp_f"] * session["humidity"]

    # Tutor formulas:
    # WX_SPOT: temp_F * humidity_% at settlement time (12pm London).
    # WX_SUM: sum(temp_F * humidity_%) over 15m points / 100.
    settle_idx = (weather["time"] - end).abs().idxmin()
    settle = weather.loc[settle_idx]
    wx_spot = c_to_f(float(settle["temperature_c"])) * float(settle["humidity"])
    wx_sum = session["wx_prod"].sum() / 100.0

    return round(wx_spot), round(wx_sum)


if __name__ == "__main__":
    print(fair_price())
