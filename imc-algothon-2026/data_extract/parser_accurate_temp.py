import re
from bs4 import BeautifulSoup
from datetime import datetime

def parse_myforecast_current(html: str):
    soup = BeautifulSoup(html, "lxml")

    # 1) datetime：use system current time
    dt_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # ---------- helpers ----------
    def clean_text(x):
        return re.sub(r"\s+", " ", x or "").strip()

    def find_number(txt):
        m = re.search(r"-?\d+(?:\.\d+)?", txt)
        return float(m.group()) if m else None

    # 2) current temp（ h1.display-2）
    temp_val = None
    temp_unit = None
    h1 = soup.select_one("h1.display-2")
    if h1:
        t = clean_text(h1.get_text(" ", strip=True))
        # t 可能像 "19 °F" / " -2 °C "
        m = re.search(r"(-?\d+(?:\.\d+)?)\s*°\s*([FC])", t, re.I)
        if m:
            temp_val = float(m.group(1))
            temp_unit = m.group(2).upper()
        else:
            # backup：only looking for number, unit from temmetric variable in script
            temp_val = find_number(t)
            # 单位兜底：从 temmetric 变量里找 "°C" 或 "°F"
            tm = soup.find("script", string=re.compile(r"var\s+temmetric", re.I))
            if tm:
                if "°C" in tm.string:
                    temp_unit = "C"
                elif "°F" in tm.string:
                    temp_unit = "F"

    # 3) 当前概况（Clear. Cold.）
    condition_text = None
    small_cond = soup.select_one(".col-12.col-lg-6.text-center small.primary-txt")
    if small_cond:
        condition_text = clean_text(small_cond.get_text())

    # 4) Humidity：找 “Humidity” 这一行对应的百分比
    humidity_pct = None
    for bold_p in soup.select("#more p.fw-bold"):
        if clean_text(bold_p.get_text()).lower() == "humidity":
            row = bold_p.find_parent(class_=re.compile(r"d-flex"))
            if row:
                ps = row.find_all("p")
                if len(ps) >= 2:
                    humidity_pct = find_number(ps[1].get_text())
            break

    # 5) Wind：形如 "Wind: 1 mph N"
    wind_mph = None
    wind_dir = None
    wind_p = None
    for p in soup.find_all("p"):
        if "Wind:" in p.get_text():
            wind_p = p
            break
    if wind_p:
        wt = clean_text(wind_p.get_text(" ", strip=True))
        # mph
        m = re.search(r"Wind:\s*([\d.]+)\s*mph", wt, re.I)
        if m:
            wind_mph = float(m.group(1))
        # 方向在 <sup> 里
        sup = wind_p.find("sup")
        if sup:
            wind_dir = clean_text(sup.get_text())

    # 6) More Details 里的其它 current 指标（有就抓）
    visibility_mi = dewpoint = pressure_in = comfort = tendency = None

    def get_kv_value(label):
        for bold_p in soup.select("#more p.fw-bold"):
            if clean_text(bold_p.get_text()).lower() == label.lower():
                row = bold_p.find_parent(class_=re.compile(r"d-flex"))
                if row:
                    ps = row.find_all("p")
                    if len(ps) >= 2:
                        return clean_text(ps[1].get_text(" ", strip=True))
        return None

    vis_txt = get_kv_value("Visibility")
    if vis_txt:
        visibility_mi = find_number(vis_txt)

    dew_txt = get_kv_value("Dew Point")
    if dew_txt:
        dewpoint = find_number(dew_txt)  # 默认和 temp_unit 同单位

    pre_txt = get_kv_value("Pressure")
    if pre_txt:
        pressure_in = find_number(pre_txt)  # inHg

    com_txt = get_kv_value("Comfort Level")
    if com_txt:
        comfort = find_number(com_txt)

    tendency = get_kv_value("Tendency")

    # 7) 输出
    out = {
        "datetime": dt_str[:-2]+"00",
        "temp_f": temp_val,
        "temp_unit": temp_unit,          # "F" or "C"
        "humidity_pct": humidity_pct,    # 93.0 这种
        "condition_text": condition_text,
        "wind_mph": wind_mph,
        "wind_dir": wind_dir,
        "visibility_mi": visibility_mi,
        "dewpoint": dewpoint,
        "pressure_in": pressure_in,
        "comfort": comfort,
        "tendency": tendency,
    }
    return out
