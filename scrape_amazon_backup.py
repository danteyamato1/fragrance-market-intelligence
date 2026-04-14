import re
import time
import random
import json
import os
import logging
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("amazon")

SITES = {
    "USA": {"domain": "https://www.amazon.com", "currency": "USD"},
    "UAE": {"domain": "https://www.amazon.ae", "currency": "AED"},
}

CHROME_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/131.0.0.0 Safari/537.36"

MIN_DELAY = 5
MAX_DELAY = 10


# ================= CLEAN ================= #

def clean_price_text(text):
    if not text:
        return text
    return text.replace("\xa0", " ").replace("Â", "").strip()


# ================= SIZE ================= #

def oz_to_ml(oz):
    return oz * 29.5735


def extract_oz(text):
    m = re.search(r"(\d+(\.\d+)?)\s*oz", text.lower())
    return float(m.group(1)) if m else None


def size_matches(title, target_ml):
    title = title.lower()

    if re.search(rf"\b{target_ml}\s*ml\b", title):
        return True

    oz = extract_oz(title)
    if oz:
        ml = oz_to_ml(oz)
        if abs(ml - target_ml) <= 5:
            return True

    return False


def ml_to_oz_string(ml):
    mapping = {
        30: "1 oz",
        50: "1.7 oz",
        60: "2 oz",
        100: "3.4 oz",
        125: "4.2 oz",
        150: "5 oz",
        200: "6.7 oz",
    }
    return mapping.get(ml)


# ================= VARIANT ================= #

VARIANT_MAP = {
    "eau de toilette": "edt",
    "edt": "edt",
    "eau de parfum": "edp",
    "edp": "edp",
    "parfum": "parfum",
    "elixir": "elixir",
}


def extract_variant(text):
    text = text.lower()
    for k, v in VARIANT_MAP.items():
        if k in text:
            return v
    return None


# ================= MATCHING ================= #

def build_product_struct(product, query):
    size_match = re.search(r"(\d+)\s*(ml|oz)", query.lower())
    size_ml = int(size_match.group(1)) if size_match else None

    return {
        "brand": product["brand"].lower(),
        "line": product["name"].split()[0].lower(),
        "variant": extract_variant(query),
        "size_ml": size_ml,
    }


def is_strong_match(title, product):
    title = title.lower()

    if product["brand"] not in title:
        return False

    if product["line"] not in title:
        return False

    if product["variant"] and product["variant"] not in title:
        return False

    if product["size_ml"] and not size_matches(title, product["size_ml"]):
        return False

    return True


# ================= PLAYWRIGHT ================= #

def _start_browser():
    from playwright.sync_api import sync_playwright
    pw = sync_playwright().start()
    browser = pw.chromium.launch(headless=True)
    return pw, browser


def _new_context(browser):
    context = browser.new_context(
        user_agent=CHROME_UA,
        viewport={"width": 1366, "height": 768},
        locale="en-US",
    )
    return context, context.new_page()


def _warmup(page, domain):
    try:
        page.goto(domain, timeout=20000)
        time.sleep(random.uniform(1.5, 3.0))
    except:
        pass


def _set_us_zip(page):
    try:
        page.click("#nav-global-location-popover-link", timeout=5000)
        time.sleep(1)
        page.fill("input[aria-label='ZIP Code']", "90210")
        page.click("input[aria-labelledby='GLUXZipUpdate-announce']")
        time.sleep(2)
        logger.info("ZIP set")
    except:
        pass


def _set_currency_cookie(page):
    try:
        page.add_init_script("""
            document.cookie = "i18n-prefs=USD";
        """)
    except:
        pass


def _fetch(page, url):
    try:
        page.goto(url, timeout=25000)
        page.wait_for_selector('[data-component-type="s-search-result"]', timeout=8000)
        return page.content()
    except:
        return None


# ================= PRICE EXTRACTION ================= #

def extract_price(card):

    # 🟢 1. STRONGEST: Look for "Price, product page" anywhere in card
    text = card.get_text(" ", strip=True)

    if "Price, product page" in text:
        m = re.search(r"Price, product page\s*(\$\s?\d[\d,\.]*)", text)
        if m:
            return m.group(1)

    # 🟢 2. Primary visible price block (Buy Box equivalent)
    price_block = card.select_one(".a-price")

    if price_block:
        price_text = price_block.get_text(" ", strip=True)

        # Avoid grabbing "per ounce" etc.
        m = re.search(r"^\$\s?\d[\d,\.]*", price_text)
        if m:
            return m.group(0)

    # 🔴 3. Explicitly IGNORE "More Buying Choices" unless nothing else exists
    if "More Buying Choices" in text and "Price, product page" not in text:
        m = re.search(r"\$\s?\d[\d,\.]*\s*\(\d+\s+new offers\)", text)
        if m:
            # only use if nothing else found
            fallback = re.search(r"\$\s?\d[\d,\.]*", m.group(0))
            return fallback.group(0) if fallback else None

    # 🟡 4. UAE fallback
    m = re.search(r"(AED\s?\d[\d,\.]*)", text)
    if m:
        return m.group(1)

    # 🟡 5. Final fallback
    m = re.search(r"(\$\s?\d[\d,\.]*)", text)
    if m:
        return m.group(1)

    return None

def normalize_price(price):
    if not price:
        return None

    price = clean_price_text(price)
    logger.info(f"Extracted price: {price}")

    m = re.search(r"(AED|\$)\s?(\d[\d,\.]*)", price)
    return f"{m.group(1)}{m.group(2)}" if m else price


# ================= SCRAPER ================= #

def scrape_one(page, query, product, site_key, scrape_date):

    search_query = query.strip()

    # 🔥 ML → OZ for USA
    if site_key == "USA":
        match = re.search(r"(\d+)\s*ml", query.lower())
        if match:
            ml = int(match.group(1))
            oz = ml_to_oz_string(ml)
            if oz:
                search_query = re.sub(r"\d+\s*ml", oz, query, flags=re.I)

    url = f"{SITES[site_key]['domain']}/s?k={search_query.replace(' ', '+')}"

    html = _fetch(page, url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    cards = soup.select('[data-component-type="s-search-result"]')

    product_struct = build_product_struct(product, query)

    best_card = None

    # strong match
    for card in cards[:15]:
        title_el = card.select_one("h2")
        if not title_el:
            continue

        title = title_el.get_text(" ", strip=True)

        if is_strong_match(title, product_struct):
            best_card = card
            break

    # fallback
    if not best_card:
        for card in cards[:5]:
            txt = card.get_text(" ", strip=True).lower()
            if product_struct["brand"] in txt and product_struct["line"] in txt:
                best_card = card
                break

    if not best_card:
        return None

    return extract_data(best_card, product, site_key, scrape_date, url)


def extract_data(card, product, site_key, scrape_date, url):
    title_el = card.select_one("h2 span")

    title = title_el.get_text(strip=True) if title_el else None
    price = normalize_price(extract_price(card))

    return {
        "scrape_date": scrape_date,
        "country": site_key,
        "name": product["name"],
        "brand": product["brand"],
        "price_raw": price,
        "amazon_title_raw": title,
        "amazon_url": url,
        "availability": "in_stock" if price else "unavailable",
    }


# ================= MAIN ================= #

def scrape_all(products, scrape_date, limit=None):
    products = products[:limit] if limit else products
    results = []

    pw, browser = _start_browser()

    try:
        for country in ["USA", "UAE"]:
            context, page = _new_context(browser)

            _warmup(page, SITES[country]["domain"])

            if country == "USA":
                _set_us_zip(page)
                _set_currency_cookie(page)

            for p in products:
                query = p.get("amazon_search", f"{p['brand']} {p['name']}")

                row = scrape_one(page, query, p, country, scrape_date)

                if row:
                    results.append(row)

                time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))

            page.close()
            context.close()

    finally:
        browser.close()
        pw.stop()

    return results


# ================= SAVE ================= #

def save_raw(data, path="raw_data/amazon_raw.csv"):
    os.makedirs(os.path.dirname(path), exist_ok=True)

    df = pd.DataFrame(data)
    exists = os.path.exists(path)

    df.to_csv(path, mode="a", header=not exists, index=False, encoding="utf-8-sig")
    logger.info(f"Saved {len(df)} rows → {path}")

    return df


# ================= CLI ================= #

if __name__ == "__main__":
    with open("products.json") as f:
        products = json.load(f)

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    data = scrape_all(products, today, limit=3)

    if data:
        df = save_raw(data)
        print(df[["name", "country", "price_raw"]])