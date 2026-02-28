from bs4 import BeautifulSoup
import json
import re

def parse_flights_departure(html: str, date="2025-11-22"):
    soup = BeautifulSoup(html, "html.parser")

    # 优先用大表（fp-flights-table-large），因为列更全
    table_large = soup.select_one("table.fp-flights-table-large")
    if table_large:
        rows = table_large.select("tbody tr.fp-flight-item")
        mode = "large"
    else:
        # 兜底：用小表（mobile）
        table_small = soup.select_one("table.fp-flights-table-small")
        rows = table_small.select("tbody tr.fp-flight-item") if table_small else []
        mode = "small"

    flights = []

    for tr in rows:
        flight_id = tr.get("data-flight-id")

        if mode == "large":
            # Airline：从 aria-label 或隐藏 info-content 里拿
            airline = None
            spacer = tr.select_one(".fp-flight-airline .spacer-2-1")
            if spacer and spacer.get("aria-label"):
                airline = spacer["aria-label"].strip()
            if not airline:
                info = tr.select_one(".fp-flight-airline .info-content")
                airline = info.get_text(strip=True) if info else None

            destination = tr.select_one(".fp-flight-airport a")
            destination = destination.get_text(" ", strip=True) if destination else None

            flight_num = tr.select_one(".fp-flight-number .nobr")
            flight_num = flight_num.get_text(strip=True) if flight_num else None

            status = tr.select_one(".fp-flight-status")
            status = status.get_text(strip=True) if status else None

            # planned | expected
            muc_time = tr.select_one(".fp-flight-time-muc")
            muc_time = muc_time.get_text(" ", strip=True) if muc_time else ""
            # 可能是 "06:05 |" 或 "06:05 | 06:20"
            planned, expected = None, None
            if muc_time:
                parts = [p.strip() for p in muc_time.split("|")]
                if len(parts) >= 1 and parts[0]:
                    planned = parts[0]
                if len(parts) >= 2 and parts[1]:
                    expected = parts[1]

            arrival = tr.select_one(".fp-flight-time-other")
            arrival = arrival.get_text(strip=True) if arrival else None

            area = tr.select_one(".fp-flight-area .nobr")
            area = area.get_text(strip=True) if area else None

        else:
            # small 表的结构不一样
            airline = None
            img = tr.select_one(".fp-flight-airline img.logo-airline")
            # small 里 airline 名字通常没有 aria-label，尝试从附近文本拿
            if img and img.get("alt"):
                airline = img["alt"].strip()

            destination = tr.select_one(".fp-flight-airport span")
            destination = destination.get_text(" ", strip=True) if destination else None

            flight_num = tr.select_one(".fp-flight-number span.nobr")
            flight_num = flight_num.get_text(strip=True) if flight_num else None

            status = tr.select_one(".time-table tr:nth-of-type(1) td:nth-of-type(2)")
            status = status.get_text(strip=True) if status else None

            planned = tr.select_one(".time-table tr:nth-of-type(2) td:nth-of-type(2)")
            planned = planned.get_text(strip=True) if planned else None

            expected = tr.select_one(".time-table tr:nth-of-type(3) td:nth-of-type(2)")
            expected = expected.get_text(strip=True) if expected else None
            expected = expected if expected else None

            arrival = tr.select_one(".time-table tr:nth-of-type(4) td:nth-of-type(2)")
            arrival = arrival.get_text(strip=True) if arrival else None

            area = tr.select_one(".time-table tr:nth-of-type(5) td:nth-of-type(2) .nobr")
            area = area.get_text(strip=True) if area else None

        # 可选：把 flight_num 拆成 航班号 + 机型
        flight_code, aircraft = None, None
        if flight_num:
            # 例如 "TP 557 (A21N)"
            m = re.match(r"^(.*?)\s*\((.*?)\)\s*$", flight_num)
            if m:
                flight_code = m.group(1).strip()
                aircraft = m.group(2).strip()
            else:
                flight_code = flight_num.strip()

        flights.append({
            "date": date,
            "flight_id": flight_id,
            "airline": airline,
            "destination": destination,
            "flight_num_raw": flight_num,
            "flight_no": flight_code,
            "aircraft_type": aircraft,
            "status": status,
            "planned_time": planned,
            "expected_time": expected,
            "arrival_time_local": arrival,
            "terminal_area": area,
        })

    return flights


if __name__ == "__main__":
    # html_str = open("flights.html", "r", encoding="utf-8").read()
    html_str = """PASTE_YOUR_HTML_HERE"""
    # data = parse_flights(html_str)

    print(json.dumps(data, ensure_ascii=False, indent=2))
