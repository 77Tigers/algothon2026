import re, json
from bs4 import BeautifulSoup
import pandas as pd

def extract_timeanddate_hourly(html: str):
    soup = BeautifulSoup(html, "html.parser")

    # 1) 找到包含 "var data=" 的 script
    script_text = None
    for s in soup.find_all("script"):
        if s.string and "var data=" in s.string:
            script_text = s.string
            break
    if script_text is None:
        raise ValueError("没找到包含 var data= 的 script")

    # 2) 正则抠出 {...} 这一坨
    m = re.search(r"var data\s*=\s*({.*?})\s*;", script_text, re.S)
    if not m:
        raise ValueError("正则没抠到 data 的 JSON")

    data_str = m.group(1)

    # 3) 解析 JSON
    data = json.loads(data_str)

    # 4) detail 是逐小时列表；里面夹着日标题 hl=true 的行，过滤掉
    detail = [
        d for d in data["detail"]
        if isinstance(d, dict) and "temp" in d and "date" in d
    ]

    df = pd.DataFrame(detail)

    # 5) 把 date(ms epoch) 转成 datetime
    df["datetime_utc"] = pd.to_datetime(df["date"], unit="ms", utc=True)

    # 6) 你常用的列整理一下
    keep = ["datetime_utc","ts","ds","temp","cf","wind","wd","hum","pc","rain","snow","desc","icon"]
    df = df[[c for c in keep if c in df.columns]]

    return df

def to_30min(df_hourly: pd.DataFrame, method="ffill"):
    """
    method:
      - "ffill"  半小时点沿用上一小时预报（更符合“预报是分段值”的含义）
      - "interp" 温度等线性插值（更平滑）
    """
    df = df_hourly.copy()
    df = df.set_index("datetime_utc").sort_index()

    if method == "ffill":
        df_30 = df.resample("30min").ffill()
    elif method == "interp":
        df_30 = df.resample("30min").interpolate("time")
        # 非数值列再 ffill 一下
        non_num = df_30.columns.difference(df_30.select_dtypes("number").columns)
        df_30[non_num] = df_30[non_num].ffill()
    else:
        raise ValueError("method 只能是 ffill 或 interp")

    df_30 = df_30.reset_index()
    return df_30


# ====== 用法 ======
# html = 你的整页 HTML 字符串
# df_hourly = extract_timeanddate_hourly(html)
# df_30 = to_30min(df_hourly, method="ffill")

# 看结果
# print(df_hourly.head())
# print(df_30.head(10))
