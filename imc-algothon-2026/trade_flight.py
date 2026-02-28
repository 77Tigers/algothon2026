from __future__ import annotations

import pandas as pd

from data_extract.heathrow_flights import get_session_flights
from data_extract.session import LONDON_TZ, settlement_session_bounds


def get_fair_flight_price() -> tuple[int, int]:
    start, end = settlement_session_bounds()
    arrivals, departures = get_session_flights(start, end)

    # Product 5: total arrivals + departures in the 24h settlement window.
    lhr_count = len(arrivals) + len(departures)

    # Product 6: sum over 30m intervals of 100 * (arr - dep) / (arr + dep).
    bins = pd.date_range(start=start, end=end, freq="30min", inclusive="left", tz=LONDON_TZ)
    arr_counts = pd.Series(0, index=bins)
    dep_counts = pd.Series(0, index=bins)

    for ts in arrivals:
        bucket = ts.floor("30min")
        if bucket in arr_counts.index:
            arr_counts.loc[bucket] += 1
    for ts in departures:
        bucket = ts.floor("30min")
        if bucket in dep_counts.index:
            dep_counts.loc[bucket] += 1

    metric = 0.0
    for bucket in bins:
        arr = int(arr_counts.loc[bucket])
        dep = int(dep_counts.loc[bucket])
        total = arr + dep
        if total > 0:
            metric += 100.0 * (arr - dep) / total

    return round(lhr_count), round(metric)


if __name__ == "__main__":
    print(get_fair_flight_price())
