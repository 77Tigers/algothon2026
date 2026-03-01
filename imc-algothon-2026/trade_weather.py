from __future__ import annotations

from datetime import datetime

import pandas as pd
from data_extract.london_weather import c_to_f, get_weather_df
from data_extract.session import LONDON_TZ, settlement_session_bounds


def fair_price() -> tuple[int, int]:
    start, end = settlement_session_bounds()
    now = datetime.now(LONDON_TZ)
    weather = get_weather_df()
    session = weather[(weather["time"] >= start) & (weather["time"] < end)].copy()
    if session.empty:
        raise RuntimeError("No London weather points in current session window")

    session["temp_f"] = session["temperature_c"].map(c_to_f).round()
    session["wx_prod"] = session["temp_f"] * session["humidity"]

    # WX_SPOT: temp_F * humidity_% at settlement time (12pm London).
    # Use forecast for settlement time (best available estimate).
    settle_idx = (weather["time"] - end).abs().idxmin()
    settle = weather.loc[settle_idx]
    wx_spot = round(c_to_f(float(settle["temperature_c"]))) * float(settle["humidity"])

    # WX_SUM: sum(temp_F * humidity_%) over 15m points / 100.
    # Split into realized (past) + blended estimate (future).
    realized = session[session["time"] <= now]
    future = session[session["time"] > now]
    total_intervals = len(session)  # expected ~96 intervals in 24h

    if len(realized) >= 2:
        realized_sum = realized["wx_prod"].sum()
        realized_avg = realized_sum / len(realized)
        if len(future) > 0:
            forecast_sum = future["wx_prod"].sum()
            forecast_avg = forecast_sum / len(future)
            # Blend: 50% realized avg + 50% forecast avg for remaining intervals
            # This accounts for diurnal patterns (forecast) while dampening forecast bias (realized)
            blended_avg = 0.5 * realized_avg + 0.5 * forecast_avg
            wx_sum = (realized_sum + blended_avg * len(future)) / 100.0
        else:
            # All intervals realized
            wx_sum = realized_sum / 100.0
    else:
        # Not enough realized data yet, use full forecast as fallback
        wx_sum = session["wx_prod"].sum() / 100.0

    return round(wx_spot), round(wx_sum)


if __name__ == "__main__":
    print(fair_price())
