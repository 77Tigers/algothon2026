import requests
from parser_weather import extract_timeanddate_hourly
burp0_url = "https://www.timeanddate.com:443/weather/uk/london/hourly"
burp0_headers = {"User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:144.0) Gecko/20100101 Firefox/144.0", "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8", "Accept-Language": "en-US,en;q=0.5", "Accept-Encoding": "gzip, deflate, br", "Upgrade-Insecure-Requests": "1", "Sec-Fetch-Dest": "document", "Sec-Fetch-Mode": "navigate", "Sec-Fetch-Site": "none", "Sec-Fetch-User": "?1", "Priority": "u=0, i", "Te": "trailers"}

import requests

def getweather():
    url = "https://www.timeanddate.com/weather/uk/london/hourly"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9,de;q=0.8",
        # 关键：不要 br
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
    }
    ret = requests.get(url, headers=headers, timeout=15)
    ret.encoding = ret.apparent_encoding
    html = ret.text

    with open("w.html", "w", encoding="utf-8") as f:
        f.write(html)

    return html


def getcurrent():
    # https://www.timeanddate.com/weather/uk/london/historic
    url = "https://www.timeanddate.com/weather/uk/london/historic"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9,de;q=0.8",
        # 关键：不要 br
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
    }
    ret = requests.get(url, headers=headers, timeout=15)
    ret.encoding = ret.apparent_encoding
    html = ret.text

    with open("w.html", "w", encoding="utf-8") as f:
        f.write(html)

    return html

    # print("status:", ret.status_code)
    # print("content-encoding:", ret.headers.get("Content-Encoding"))
    # print("content-type:", ret.headers.get("Content-Type"))

    # 让 requests 按正确编码解码
   


# df = extract_timeanddate_hourly(getweather())
# print(getweather())
# print(getcurrent())
# df2 = extract_timeanddate_hourly(getcurrent())
# print(df.head(10))
    # 也可以保存
# df.to_csv("london_hourly_2.csv", index=False)
# df2.to_csv("london_current_2.csv", index=False)
# print("saved to london_hourly.csv")
