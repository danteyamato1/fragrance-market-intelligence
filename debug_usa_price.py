"""debug_usa_price.py — Dumps secondary-offer and all price-related text from amazon.com"""
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

URL = "https://www.amazon.com/s?k=Dior+Sauvage+100ml"

with sync_playwright() as pw:
    browser = pw.chromium.launch(headless=True, args=["--no-sandbox"])
    context = browser.new_context(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        viewport={"width": 1280, "height": 800},
        locale="en-US",
        extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
    )
    page = context.new_page()
    page.goto(URL, timeout=30000, wait_until="domcontentloaded")
    try:
        page.wait_for_selector('[data-component-type="s-search-result"]', timeout=10000)
    except Exception:
        pass
    try:
        page.wait_for_selector('h2 a span', timeout=10000)
    except Exception:
        pass
    html = page.content()
    browser.close()

soup  = BeautifulSoup(html, "html.parser")
cards = soup.select('[data-component-type="s-search-result"]')
print(f"Cards: {len(cards)}\n")

for i, card in enumerate(cards[:5]):
    title = card.select_one("h2 a span")
    print(f"--- Card {i}: {title.get_text(strip=True)[:80] if title else 'NO TITLE'}")

    all_text = card.get_text(" ", strip=True)
    dollar_parts = [w for w in all_text.split() if "$" in w]
    print(f"  $ tokens in card: {dollar_parts[:10]}")

    sec = card.select_one('[data-cy="secondary-offer-recipe"]')
    if sec:
        print(f"  secondary-offer text: {sec.get_text(' ', strip=True)[:200]!r}")
        for j, span in enumerate(sec.find_all("span")):
            t = span.get_text(strip=True)
            if t:
                print(f"    span[{j}]: {t!r}  bytes={t.encode('utf-8')[:4].hex()}")
    else:
        print("  secondary-offer: NOT PRESENT")

    recipe = card.select_one('[data-cy="price-recipe"]')
    if recipe:
        print(f"  price-recipe text: {recipe.get_text(' ', strip=True)[:100]!r}")
    print()
