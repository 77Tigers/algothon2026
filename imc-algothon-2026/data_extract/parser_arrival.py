from bs4 import BeautifulSoup
import re
from datetime import datetime
import pandas as pd

def clean_text(x: str) -> str:
    if not x:
        return ""
    return re.sub(r"\s+", " ", x.replace("\xa0", " ")).strip()

def parse_flight_no_and_type(text: str):
    """
    exmaple: "LH 683 (A21N)" -> ("LH 683", "A21N")
        "LH 683"        -> ("LH 683", "")
    """
    text = clean_text(text)
    m = re.match(r"(.+?)\s*\(([^)]+)\)", text)
    if m:
        return clean_text(m.group(1)), clean_text(m.group(2))
    return text, ""

def extract_date_from_header(soup: BeautifulSoup) -> str:
    """
    Fetch from h3.fp-flights-headline :
    "Flüge nach München am 22.11.2025" -> "2025-11-22"
    none return empty string
    """
    h3 = soup.select_one("h3.fp-flights-headline")
    if not h3:
        return ""
    txt = clean_text(h3.get_text(" ", strip=True))
    m = re.search(r"(\d{2}\.\d{2}\.\d{4})", txt)
    if not m:
        return ""
    dt = datetime.strptime(m.group(1), "%d.%m.%Y")
    return dt.strftime("%Y-%m-%d")

def parse_large_row(row, date_str: str):
    tds = row.find_all("td", recursive=False)
    if len(tds) < 7:
        return None

    # flight_id 保持字符串
    flight_id = clean_text(row.get("data-flight-id", ""))

    # airline: aria-label 优先
    airline_td = tds[0]
    airline = ""
    spacer = airline_td.find(class_=re.compile(r"spacer"))
    if spacer and spacer.get("aria-label"):
        airline = spacer["aria-label"]
    if not airline:
        info = airline_td.select_one(".info-content")
        if info:
            airline = info.get_text(strip=True)
    airline = clean_text(airline)

    # origin (Von)
    origin = clean_text(tds[1].get_text(" ", strip=True))

    # flight no + aircraft
    flight_text = clean_text(tds[2].get_text(" ", strip=True))
    flight_no, aircraft_type = parse_flight_no_and_type(flight_text)

    # status
    status = clean_text(tds[3].get_text(" ", strip=True))

    # departure time from origin (Abflug)
    dep_other = clean_text(tds[4].get_text(" ", strip=True))

    # MUC planned | expected
    muc_time = clean_text(tds[5].get_text(" ", strip=True))
    planned_muc, expected_muc = "", ""
    if "|" in muc_time:
        parts = [clean_text(p) for p in muc_time.split("|")]
        planned_muc = parts[0] if len(parts) > 0 else ""
        expected_muc = parts[1] if len(parts) > 1 else ""
    else:
        planned_muc = muc_time

    # area / terminal
    area = clean_text(tds[6].get_text(" ", strip=True))

    return {
        "flight_id": flight_id,
        "date": date_str,
        "airline": airline,
        "origin": origin,
        "flight_no": flight_no,
        "aircraft_type": aircraft_type,
        "status": status,
        "departure_time": dep_other,
        "planned_time": planned_muc,
        "expected_time": expected_muc,
        "area": area,
        "source": "large",
    }

def parse_small_row(row, date_str: str):
    flight_id = clean_text(row.get("data-flight-id", ""))

    details_td = row.find("td", class_="fp-flight-details")
    data_td = row.find("td", class_="fp-flight-data")
    if not details_td or not data_td:
        return None

    # airline: small 表只有 logo，试试 alt/title
    airline = ""
    img = details_td.find("img")
    if img:
        airline = clean_text(img.get("alt") or img.get("title") or "")
    airline = clean_text(airline)

    # flight number & origin
    fn_node = details_td.select_one(".fp-flight-number")
    ap_node = details_td.select_one(".fp-flight-airport")
    flight_text = clean_text(fn_node.get_text(" ", strip=True)) if fn_node else ""
    origin = clean_text(ap_node.get_text(" ", strip=True)) if ap_node else ""
    flight_no, aircraft_type = parse_flight_no_and_type(flight_text)

    # time-table 里的 key/value
    status = planned_muc = expected_muc = dep_other = area = via = ""
    for tr in data_td.select("table.time-table tr"):
        tds = tr.find_all("td")
        if len(tds) < 2:
            continue
        k = clean_text(tds[0].get_text(" ", strip=True)).lower()
        v = clean_text(tds[1].get_text(" ", strip=True))

        if k == "via":
            via = v
        elif k == "status":
            status = v
        elif k == "abflug":
            dep_other = v
        elif k == "geplant":
            planned_muc = v
        elif k == "erwartet":
            expected_muc = v
        elif k == "bereich":
            area = v

    # small 表有 via 时，把 origin 补成 "XXX via YYY" 方便对齐
    if via:
        origin = f"{origin} via {via}".strip()

    return {
        "flight_id": flight_id,
        "date": date_str,
        "airline": airline,
        "origin": origin,
        "flight_no": flight_no,
        "aircraft_type": aircraft_type,
        "status": status,
        "departure_time": dep_other,
        "planned_time": planned_muc,
        "expected_time": expected_muc,
        "area": area,
        "source": "small",
    }

def parse_flights_arrival(html: str, date="2025-11-22"):
    soup = BeautifulSoup(html, "html.parser")
    date_str = extract_date_from_header(soup)

    flights = []

    # large 表
    for row in soup.select("table.fp-flights-table-large tr.fp-flight-item"):
        item = parse_large_row(row, date)
        if item:
            flights.append(item)

    # small 表
    for row in soup.select("table.fp-flights-table-small tr.fp-flight-item"):
        item = parse_small_row(row, date)
        if item:
            flights.append(item)

    # map 去重：同一 flight_id + flight_no 视为同一条，优先 large
    dedup = {}
    for f in flights:
        key = (f.get("flight_id"), f.get("flight_no"))
        if key not in dedup or dedup[key]["source"] == "small":
            dedup[key] = f

    return list(dedup.values())

# demo
if __name__ == "__main__":
    html = open("html1.html", "r", encoding="utf-8").read()
    # data = parse_flights(html)
    # df = pd.DataFrame(data)
    # print(df.head())
    # df.to_csv("muc_arrivals.csv", index=False, encoding="utf-8-sig")
