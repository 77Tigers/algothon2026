from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import os
import time
from time import sleep

from data_extract.thames_tide import get_thames_fair_price
from trade_flight import get_fair_flight_price
from trade_weather import fair_price

FLIGHT_REFRESH_SECONDS = int(os.getenv("FLIGHT_REFRESH_SECONDS", "1800"))
FLIGHT_RETRY_AFTER_FAIL_SECONDS = int(os.getenv("FLIGHT_RETRY_AFTER_FAIL_SECONDS", "3600"))
cached_flights: tuple[int, int] | None = None
last_flight_refresh = 0.0


def write_fps(payload: dict) -> None:
    with open("fps.json", "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def load_cached_flights_from_fps(path: str = "fps.json") -> tuple[int, int] | None:
    try:
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        count = payload.get("LHR_COUNT")
        index = payload.get("LHR_INDEX")
        if isinstance(count, int) and isinstance(index, int):
            return (count, index)
    except Exception:
        return None
    return None


cached_flights = load_cached_flights_from_fps()
if cached_flights is not None:
    print(f"Loaded cached flights from fps.json: LHR_COUNT={cached_flights[0]}, LHR_INDEX={cached_flights[1]}")


while True:
    fp: dict[str, int | str] = {}
    wx_spot, wx_sum = 0, 0
    tide_spot, tide_swing = 0, 0
    lhr_count, lhr_index = 0, 0

    now = time.time()
    should_refresh_flights = cached_flights is None or (now - last_flight_refresh) >= FLIGHT_REFRESH_SECONDS

    print("Fetching weather/tide data in parallel")
    futures: dict = {}
    with ThreadPoolExecutor(max_workers=3) as pool:
        futures[pool.submit(fair_price)] = "weather"
        futures[pool.submit(get_thames_fair_price, True)] = "tide"

        if should_refresh_flights:
            print("Fetching flight data")
            futures[pool.submit(get_fair_flight_price)] = "flights"
        else:
            print("Using cached flight data")
            lhr_count, lhr_index = cached_flights if cached_flights is not None else (0, 0)
            fp["LHR_COUNT"] = lhr_count
            fp["LHR_INDEX"] = lhr_index
            fp["5_Flights"] = lhr_count
            fp["6_Airport"] = lhr_index
            print(f"Flights done: LHR_COUNT={lhr_count}, LHR_INDEX={lhr_index}")
            write_fps(fp)

        for future in as_completed(futures):
            source = futures[future]
            try:
                result = future.result()
                if source == "weather":
                    wx_spot, wx_sum = result
                    fp["WX_SPOT"] = wx_spot
                    fp["WX_SUM"] = wx_sum
                    fp["3_Weather"] = wx_spot
                    fp["4_Weather"] = wx_sum
                    print(f"Weather done: WX_SPOT={wx_spot}, WX_SUM={wx_sum}")
                elif source == "tide":
                    tide_spot, tide_swing = result
                    fp["TIDE_SPOT"] = tide_spot
                    fp["TIDE_SWING"] = tide_swing
                    fp["1_Tide"] = tide_spot
                    fp["2_Tide"] = tide_swing
                    print(f"Tide done: TIDE_SPOT={tide_spot}, TIDE_SWING={tide_swing}")
                elif source == "flights":
                    cached_flights = result
                    last_flight_refresh = now
                    lhr_count, lhr_index = cached_flights
                    fp["LHR_COUNT"] = lhr_count
                    fp["LHR_INDEX"] = lhr_index
                    fp["5_Flights"] = lhr_count
                    fp["6_Airport"] = lhr_index
                    print(f"Flights done: LHR_COUNT={lhr_count}, LHR_INDEX={lhr_index}")
            except Exception as exc:
                if source == "weather":
                    fp["WX_ERROR"] = str(exc)
                    print(f"Weather fetch failed: {exc}")
                    print("Weather fallback: WX_SPOT=0, WX_SUM=0")
                elif source == "tide":
                    fp["TIDE_ERROR"] = str(exc)
                    print(f"Tide fetch failed: {exc}")
                    print("Tide fallback: TIDE_SPOT=0, TIDE_SWING=0")
                elif source == "flights":
                    print(f"Flight fetch failed: {exc}")
                    if cached_flights is None:
                        cached_flights = (0, 0)
                    fp["FLIGHT_ERROR"] = str(exc)
                    # Back off after failure to avoid hammering the API.
                    last_flight_refresh = now - FLIGHT_REFRESH_SECONDS + FLIGHT_RETRY_AFTER_FAIL_SECONDS
                    lhr_count, lhr_index = cached_flights
                    fp["LHR_COUNT"] = lhr_count
                    fp["LHR_INDEX"] = lhr_index
                    fp["5_Flights"] = lhr_count
                    fp["6_Airport"] = lhr_index
                    print(f"Flights fallback: LHR_COUNT={lhr_count}, LHR_INDEX={lhr_index}")

            write_fps(fp)

    if should_refresh_flights and "LHR_COUNT" not in fp:
        lhr_count, lhr_index = cached_flights if cached_flights is not None else (0, 0)
        fp["LHR_COUNT"] = lhr_count
        fp["LHR_INDEX"] = lhr_index
        fp["5_Flights"] = lhr_count
        fp["6_Airport"] = lhr_index
        print(f"Flights done: LHR_COUNT={lhr_count}, LHR_INDEX={lhr_index}")
        write_fps(fp)

    if not should_refresh_flights:
        lhr_count, lhr_index = cached_flights if cached_flights is not None else (0, 0)

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
    print(f"Final done: LON_ETF={fp['LON_ETF']}, LON_FLY={fp['LON_FLY']}")
    write_fps(fp)
    sleep(30)
