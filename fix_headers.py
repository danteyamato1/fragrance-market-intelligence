"""
fix_headers.py — Two things:
1. Show where the 12 tabular-nums spans are in the raw cloudscraper HTML
2. Try different request headers to get the server-side rendered (SSR) version

Usage:  python fix_headers.py
"""
import re
import cloudscraper
from bs4 import BeautifulSoup

BASE_URL = "https://www.fragrantica.com/perfume/Dior/Fahrenheit-228.html"

def try_fetch(label, extra_headers=None):
    scraper = cloudscraper.create_scraper(
        browser={"browser": "chrome", "platform": "windows", "desktop": True}
    )
    if extra_headers:
        scraper.headers.update(extra_headers)
    resp = scraper.get(BASE_URL, timeout=20)
    html = resp.text
    soup = BeautifulSoup(html, "html.parser")

    tn = html.count("tabular-nums")
    fci = html.count("flex-col items-center")
    dv  = html.count("data-v-643d6550")
    vue = html.count("likes-rating-new")

    cards = soup.find_all(class_="tw-rating-card")
    card_blocks = 0
    for c in cards:
        card_blocks += len(c.select("div.flex-col.items-center"))

    print(f"\n[{label}]")
    print(f"  tabular-nums={tn}  flex-col-items-center={fci}  data-v={dv}  vue-placeholder={vue}")
    print(f"  tw-rating-card count={len(cards)}  blocks-with-data={card_blocks}")
    return html, card_blocks > 0

html, success = try_fetch("Default cloudscraper")

if not success:

    print("\n[Locating existing tabular-nums spans in raw HTML]")
    soup = BeautifulSoup(html, "html.parser")
    for i, span in enumerate(soup.find_all("span", class_="tabular-nums")):
        parent_classes = span.parent.get("class", [])
        gp = span.parent.parent
        gp_classes = gp.get("class", []) if gp else []
        print(f"  [{i}] text={span.get_text(strip=True)!r:8} parent={parent_classes} grandparent={gp_classes[:3]}")

_, success = try_fetch("With Accept + Language headers", {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
})

_, success = try_fetch("With Referer header", {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.fragrantica.com/",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
})

print("\n[Attempt 4: raw requests with full Chrome headers]")
import requests
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Sec-Ch-Ua": '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
}
try:
    r = requests.get(BASE_URL, headers=headers, timeout=20)
    html4 = r.text
    soup4 = BeautifulSoup(html4, "html.parser")
    cards4 = soup4.find_all(class_="tw-rating-card")
    blocks4 = sum(len(c.select("div.flex-col.items-center")) for c in cards4)
    dv4 = html4.count("data-v-643d6550")
    tn4 = html4.count("tabular-nums")
    print(f"  status={r.status_code} tabular-nums={tn4} data-v={dv4} blocks={blocks4}")
except Exception as e:
    print(f"  Failed: {e}")

print("\n[DONE]")
