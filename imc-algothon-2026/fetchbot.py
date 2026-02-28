from trade_weather import fair_price
from trade_flight import get_fair_flight_price
from trade_tide import fair_price as fair_tide_price
from time import sleep
import json
import os
import time

FLIGHT_REFRESH_SECONDS = int(os.getenv("FLIGHT_REFRESH_SECONDS", "1800"))
FLIGHT_RETRY_AFTER_FAIL_SECONDS = int(os.getenv("FLIGHT_RETRY_AFTER_FAIL_SECONDS", "3600"))
cached_flights: tuple[int, int] | None = None
last_flight_refresh = 0.0

while True:
    fp = {}
    print("Fetching weather data")
    wx_spot, wx_sum = fair_price()
    fp["WX_SPOT"] = wx_spot
    fp["WX_SUM"] = wx_sum
    fp["3_Weather"] = wx_spot
    fp["4_Weather"] = wx_sum

    print("Fetching tide data")
    tide_spot, tide_swing = fair_tide_price()
    fp["TIDE_SPOT"] = tide_spot
    fp["TIDE_SWING"] = tide_swing
    fp["1_Tide"] = tide_spot
    fp["2_Tide"] = tide_swing

    now = time.time()
    if cached_flights is None or (now - last_flight_refresh) >= FLIGHT_REFRESH_SECONDS:
        print("Fetching flight data")
        try:
            cached_flights = get_fair_flight_price()
            last_flight_refresh = now
        except Exception as exc:
            print(f"Flight fetch failed: {exc}")
            if cached_flights is None:
                cached_flights = (0, 0)
            # Back off after failure to avoid hammering the API.
            last_flight_refresh = now - FLIGHT_REFRESH_SECONDS + FLIGHT_RETRY_AFTER_FAIL_SECONDS
    else:
        print("Using cached flight data")

    lhr_count, lhr_index = cached_flights
    fp["LHR_COUNT"] = lhr_count
    fp["LHR_INDEX"] = lhr_index
    fp["5_Flights"] = lhr_count
    fp["6_Airport"] = lhr_index

    lon_etf = tide_spot + wx_spot + lhr_count
    # LON_FLY = 2*Put(6200) + Call(6200) - 2*Call(6600) + 3*Call(7000)
    put_6200 = max(0, 6200 - lon_etf)
    call_6200 = max(0, lon_etf - 6200)
    call_6600 = max(0, lon_etf - 6600)
    call_7000 = max(0, lon_etf - 7000)
    lon_fly = 2 * put_6200 + call_6200 - 2 * call_6600 + 3 * call_7000

    fp["LON_ETF"] = round(lon_etf)
    fp["LON_FLY"] = round(lon_fly)
    fp["7_ETF"] = round(lon_etf)
    fp["8_Option"] = round(lon_fly)

    with open("fps.json", "w", encoding="utf-8") as f:
        json.dump(fp, f, ensure_ascii=False, indent=2)
    sleep(30)
