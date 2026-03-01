from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import math
import os
import time
from datetime import datetime
from time import sleep

from scipy.stats import norm

from data_extract.thames_tide import get_thames_fair_price
from data_extract.session import LONDON_TZ, settlement_session_bounds
from trade_flight import get_fair_flight_price
from trade_weather import fair_price


def expected_call(mu: float, sigma: float, strike: float) -> float:
    """E[max(0, S-K)] where S ~ N(mu, sigma)."""
    if sigma <= 0:
        return max(0.0, mu - strike)
    d = (mu - strike) / sigma
    return (mu - strike) * norm.cdf(d) + sigma * norm.pdf(d)


def expected_put(mu: float, sigma: float, strike: float) -> float:
    """E[max(0, K-S)] where S ~ N(mu, sigma)."""
    if sigma <= 0:
        return max(0.0, strike - mu)
    d = (strike - mu) / sigma
    return (strike - mu) * norm.cdf(d) + sigma * norm.pdf(d)


def calc_lon_fly(etf_mu: float, etf_sigma: float) -> float:
    """LON_FLY = 2*Put(6200) + Call(6200) - 2*Call(6600) + 3*Call(7000)."""
    return (
        2 * expected_put(etf_mu, etf_sigma, 6200)
        + expected_call(etf_mu, etf_sigma, 6200)
        - 2 * expected_call(etf_mu, etf_sigma, 6600)
        + 3 * expected_call(etf_mu, etf_sigma, 7000)
    )


def estimate_etf_sigma() -> float:
    """Estimate ETF settlement uncertainty based on time remaining.
    Decays with sqrt(time). sigma=0 at settlement."""
    _, end = settlement_session_bounds()
    now = datetime.now(LONDON_TZ)
    hours_left = max(0, (end - now).total_seconds() / 3600)
    time_frac = min(1.0, hours_left / 24.0)
    return 400 * math.sqrt(time_frac)


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


# Keep last known good values across loop iterations (don't reset to 0 on failure)
wx_spot, wx_sum = 0, 0
tide_spot, tide_swing = 0, 0
lhr_count, lhr_index = 0, 0

while True:
    fp: dict[str, int | str] = {}

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
    if tide_spot > 0 and wx_spot > 0 and lhr_count > 0:
        etf_sigma = estimate_etf_sigma()
        lon_fly = calc_lon_fly(lon_etf, etf_sigma)
        lon_fly_intrinsic = (
            2 * max(0, 6200 - lon_etf) + max(0, lon_etf - 6200)
            - 2 * max(0, lon_etf - 6600) + 3 * max(0, lon_etf - 7000)
        )
        fp["LON_ETF"] = round(lon_etf)
        fp["LON_FLY"] = round(lon_fly)
        fp["7_ETF"] = round(lon_etf)
        fp["8_Option"] = round(lon_fly)
        print(f"LON_ETF={round(lon_etf)} (tide={tide_spot} wx={wx_spot} flights={lhr_count})")
        print(f"LON_FLY: sigma={etf_sigma:.0f} intrinsic={lon_fly_intrinsic:.0f} expected={lon_fly:.0f}")
    else:
        print(f"Skipping ETF/FLY: components incomplete "
              f"(tide={tide_spot}, wx={wx_spot}, flights={lhr_count})")
    write_fps(fp)
    sleep(10)
