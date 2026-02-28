import requests

burp0_headers = {"User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:145.0) Gecko/20100101 Firefox/145.0", "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8", "Accept-Language": "en-US,en;q=0.5", "Accept-Encoding": "gzip, deflate, br", "Upgrade-Insecure-Requests": "1", "Sec-Fetch-Dest": "document", "Sec-Fetch-Mode": "navigate", "Sec-Fetch-Site": "none", "Sec-Fetch-User": "?1", "Priority": "u=0, i", "Te": "trailers", "Connection": "keep-alive"}

def getlevel():
    burp0_url = "https://www.hnd.bayern.de:443/pegel/isar/muenchen-himmelreichbruecke-16515005/tabelle?setdiskr=15"
    response = requests.get(burp0_url, headers=burp0_headers)
    return response.content.decode('utf-8')

def getflow():
    burp0_url = "https://www.hnd.bayern.de:443/pegel/isar/muenchen-himmelreichbruecke-16515005/tabelle?methode=abfluss&setdiskr=15"
    response = requests.get(burp0_url, headers=burp0_headers)
    return response.content.decode('utf-8')

print(getflow())