# -*- coding: utf-8 -*-
"""
从 timeanddate past weather HTML 中提取 #wt-his 表格里的每半小时观测（原样，不对齐）。
用法:
    python past24h_raw_30min_fix.py munich.html

输出:
    past24h_raw_30min.csv
"""

import re
import sys
from datetime import datetime

import pandas as pd
from bs4 import BeautifulSoup


def get_year_from_detail(html: str) -> int:
    """
    从 data.detail 的 ds 字段里抓年份（例如 'Friday, 21 November 2025, ...'）
    只用来补全表格里的 'Sat, 22 Nov' 这种无年份日期。
    """
    m = re.search(r"var\s+data\s*=\s*({.*?});\s*</script>", html, re.S)
    if not m:
        return datetime.utcnow().year

    js_obj = m.group(1)
    try:
        import json5
        data = json5.loads(js_obj)
        detail = data.get("detail", [])
        for row in detail:
            ds = row.get("ds", "")
            ym = re.search(r"\b(20\d{2})\b", ds)
            if ym:
                return int(ym.group(1))
    except Exception:
        pass

    return datetime.utcnow().year


def parse_kmh(text: str):
    m = re.search(r"([0-9.]+)\s*km/h", text)
    return float(m.group(1)) if m else None


def parse_percent(text: str):
    m = re.search(r"([0-9.]+)\s*%", text)
    return float(m.group(1)) if m else None


def parse_mbar(text: str):
    m = re.search(r"([0-9.]+)\s*mbar", text)
    return float(m.group(1)) if m else None


def parse_km(text: str):
    m = re.search(r"([0-9.]+)\s*km", text)
    return float(m.group(1)) if m else None


def extract_wt_his_30min(html: str) -> pd.DataFrame:
    soup = BeautifulSoup(html, "lxml")
    year = get_year_from_detail(html)

    table = soup.select_one("table#wt-his")
    if not table:
        raise ValueError("没找到 table#wt-his（半小时历史表）")

    rows = table.select("tbody tr")
    out = []

    current_date = None  # 例如 "Sat, 22 Nov"
    for tr in rows:
        th = tr.find("th")
        tds = tr.find_all("td")
        if not th or len(tds) < 6:
            continue

        # 时间 +（可能的）日期
        time_text = th.get_text(" ", strip=True)  # "03:20 Sat, 22 Nov" 或 "02:50"
        # span.smaller.soft 里有日期的话更新 current_date
        date_span = th.select_one("span.smaller.soft")
        if date_span:
            current_date = date_span.get_text(strip=True)  # "Sat, 22 Nov"

        # 如果 time_text 里自己带日期，也能更新
        dm = re.search(r"(Mon|Tue|Wed|Thu|Fri|Sat|Sun),\s*\d+\s*\w+", time_text)
        if dm:
            current_date = dm.group(0)

        if not current_date:
            # 没有日期就跳过（理论上不会发生）
            continue

        # 取纯时间（比如 "03:20"）
        tm = re.match(r"^\d{2}:\d{2}", time_text)
        if not tm:
            continue
        hhmm = tm.group(0)

        # 拼完整 datetime（本地日期 + 年份 + 时间）
        # current_date 例: "Sat, 22 Nov"
        dt_str = f"{current_date} {year} {hhmm}"
        dt_local = pd.to_datetime(dt_str, format="%a, %d %b %Y %H:%M", errors="coerce")
        if pd.isna(dt_local):
            continue

        # 各列
        temp_text = tds[1].get_text(" ", strip=True)  # "-6 °C"
        temp_m = re.search(r"([-\d.]+)", temp_text)
        temp_c = float(temp_m.group(1)) if temp_m else None

        weather_desc = tds[2].get_text(" ", strip=True)

        wind_text = tds[3].get_text(" ", strip=True)
        wind_kmh = parse_kmh(wind_text)

        humidity_text = tds[5].get_text(" ", strip=True)
        hum = parse_percent(humidity_text)

        baro_text = tds[6].get_text(" ", strip=True)
        baro = parse_mbar(baro_text)

        vis_text = tds[7].get_text(" ", strip=True) if len(tds) > 7 else ""
        visibility_km = parse_km(vis_text)

        out.append({
            "datetime_local": dt_local,
            "temp_c": temp_c,
            "weather": weather_desc,
            "wind_kmh": wind_kmh,
            "humidity_pct": hum,
            "baro_mbar": baro,
            "visibility_km": visibility_km
        })

    df = pd.DataFrame(out).sort_values("datetime_local")

    # 只保留最近24小时
    if not df.empty:
        end = df["datetime_local"].max()
        start = end - pd.Timedelta(hours=24)
        df = df[df["datetime_local"].between(start, end)]

    return df.reset_index(drop=True)


def main():
    if len(sys.argv) < 2:
        print("用法: python past24h_raw_30min_fix.py <html_file>")
        sys.exit(1)

    html_path = sys.argv[1]
    html = open(html_path, "r", encoding="utf-8", errors="ignore").read()

    df = extract_wt_his_30min(html)
    if df.empty:
        print("提取失败：没有半小时数据")
        sys.exit(2)

    df.to_csv("past24h_raw_30min.csv", index=False, encoding="utf-8-sig")
    print("OK! 已保存 past24h_raw_30min.csv")
    print(df.head(10))


if __name__ == "__main__":
    main()
