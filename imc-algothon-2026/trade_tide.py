from __future__ import annotations

from data_extract.session import settlement_session_bounds
from data_extract.thames_tide import get_thames


def fair_price() -> tuple[int, int]:
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

    return round(tide_spot), round(tide_swing)


if __name__ == "__main__":
    print(fair_price())
