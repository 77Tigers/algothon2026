from bs4 import BeautifulSoup
import pandas as pd
import re

def parse_waterlevel(html: str, parse_datetime: bool = True) -> pd.DataFrame:
    """
    从给定的 HND Pegel 页面 HTML 字符串中提取水位表（Datum, Wasserstand cm）。

    参数
    ----
    html : str
        你贴的整段HTML字符串
    parse_datetime : bool, default True
        是否把 Datum 解析成 pandas datetime

    返回
    ----
    df : pd.DataFrame
        列: ["datetime", "water_cm"]
        顺序与网页一致（通常最新在最上面）
    """
    soup = BeautifulSoup(html, "html.parser")

    table = soup.find("table", class_="tblsort")
    if table is None or table.tbody is None:
        raise ValueError("未找到水位表 table.tblsort 或 tbody")

    rows = []
    for tr in table.tbody.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 2:
            continue

        dt_str = tds[0].get_text(strip=True)
        level_str = tds[1].get_text(strip=True)

        try:
            level = int(level_str)
        except ValueError:
            continue

        rows.append((dt_str, level))

    if not rows:
        raise ValueError("没有解析到有效数据")

    df = pd.DataFrame(rows, columns=["datetime", "water_cm"])

    if parse_datetime:
        df["datetime"] = pd.to_datetime(
            df["datetime"],
            format="%d.%m.%Y %H:%M",
            errors="coerce"
        )

    return df

def parse_flow(html: str, parse_datetime: bool = True) -> pd.DataFrame:
    """
    从 HND Pegel 页面 HTML 字符串中提取时间序列表（Wasserstand 或 Abfluss）。

    自动识别第二列表头（比如 'Wasserstand cm' 或 'Abfluss m³/s'），
    并把德式小数逗号转成 float。

    返回 DataFrame:
      - datetime: 时间
      - value: 数值（float / int）
      - field: 字段名（wasserstand 或 abfluss）
      - unit: 单位（cm 或 m³/s）
    """
    soup = BeautifulSoup(html, "html.parser")

    table = soup.find("table", class_="tblsort")
    if table is None or table.tbody is None:
        raise ValueError("未找到表格 table.tblsort 或 tbody")

    # 读表头，识别字段和单位
    ths = table.thead.find_all("th") if table.thead else []
    second_header = ths[1].get_text(" ", strip=True) if len(ths) >= 2 else "value"

    header_lower = second_header.lower()
    if "wasserstand" in header_lower:
        field = "wasserstand"
    elif "abfluss" in header_lower:
        field = "abfluss"
    else:
        field = "value"

    # 尝试抽单位
    # e.g. "Wasserstand cm über Pegelnullpunkt" -> cm
    # e.g. "Abfluss m³/s" -> m³/s
    unit_match = re.search(r"(cm|m³/s|m3/s|m\^3/s)", second_header)
    unit = unit_match.group(1) if unit_match else None

    rows = []
    for tr in table.tbody.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 2:
            continue

        dt_str = tds[0].get_text(strip=True)
        val_str = tds[1].get_text(strip=True)
        # print(val_str)
        # 德式小数：26,1 -> 26.1
        val_str = val_str.replace(".", "").replace(",", ".")
        try:
            val = float(val_str)
        except ValueError:
            continue

        rows.append((dt_str, val))

    if not rows:
        raise ValueError("没有解析到有效数据")

    df = pd.DataFrame(rows, columns=["datetime", "value"])

    if parse_datetime:
        df["datetime"] = pd.to_datetime(
            df["datetime"],
            format="%d.%m.%Y %H:%M",
            errors="coerce"
        )

    df["field"] = field
    df["unit"] = unit

    return df


# 示例
if __name__ == "__main__":
    html = open("water.html","r",encoding='utf-8').read()
    df = parse_flow(html)
    print(df.head())
    print("最新一条：", df.iloc[0].to_dict())

