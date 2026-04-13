"""
check_frag_prices.py — Inspect what price data Fragrantica shows on a perfume page.
Run this in your project folder:  python check_frag_prices.py
"""
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import re

URL = "https://www.fragrantica.com/perfume/Dior/Sauvage-33803.html"

pw      = sync_playwright().start()
browser = pw.chromium.launch(headless=True)
context = browser.new_context(
    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    viewport={"width": 1280, "height": 800},
    locale="en-US",
)
page = context.new_page()

print(f"Fetching {URL} ...")
page.goto(URL, wait_until="domcontentloaded")
page.wait_for_timeout(5000)
html = page.content()
context.close(); browser.close(); pw.stop()

print(f"HTML length: {len(html):,}\n")
soup = BeautifulSoup(html, "html.parser")


print("=" * 60)
print("[1] Currency symbols in page text")
text = soup.get_text(" ", strip=True)
for m in re.finditer(r'[\$£€][\s]?\d[\d,\.]*|\d[\d,\.]*\s*(?:USD|GBP|EUR|usd|gbp|eur)', text):
    start = max(0, m.start() - 60)
    print(f"  ...{text[start:m.end()+60]}...")


print("\n" + "=" * 60)
print("[2] Elements with 'price' in class/id")
for el in soup.find_all(class_=re.compile("price", re.I)):
    print(f"  <{el.name} class={el.get('class')}> {el.get_text(strip=True)[:80]}")
for el in soup.find_all(id=re.compile("price", re.I)):
    print(f"  <{el.name} id={el.get('id')}> {el.get_text(strip=True)[:80]}")

print("\n" + "=" * 60)
print("[3] Buy / shop / retailer links")
for a in soup.find_all("a", href=True):
    href = a["href"]
    txt  = a.get_text(strip=True)
    if any(x in href.lower() or x in txt.lower() for x in ["buy", "shop", "price", "offer", "amazon", "sephora", "boots", "notino"]):
        print(f"  {txt[:50]:50} → {href[:80]}")


print("\n" + "=" * 60)
print("[4] Price-value widget elements")
for el in soup.find_all(class_=re.compile("price.value|value.price", re.I)):
    print(f"  {el.get_text(strip=True)[:100]}")

if "perfume-price" in html or "price-value" in html.lower():
    idx = html.lower().find("price-value")
    print(f"\n  Found 'price-value' at char {idx}:")
    print(html[max(0,idx-100):idx+300])

print("\n[DONE]")
