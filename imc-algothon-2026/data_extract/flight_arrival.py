from __future__ import annotations

import requests

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.8",
}


def getarrival(date: str | None = None) -> str:
    """Fetch Heathrow arrivals page HTML.

    The `date` argument is kept for compatibility with older call sites.
    """
    _ = date
    resp = requests.get("https://www.heathrow.com/arrivals", headers=DEFAULT_HEADERS, timeout=25)
    resp.raise_for_status()
    return resp.text
