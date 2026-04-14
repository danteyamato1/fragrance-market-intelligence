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

NON_FRAG_RE = re.compile(
    r"\b("

    r"roll[\s-]?on|roller|"
    r"fragrance\s+oil|body\s+oil|perfume\s+oil|essential\s+oil|"
    r"diffuser|reed\s+diffuser|"
    r"inspired\s+by|our\s+impression|our\s+version|"
    r"type\s+(of|for)|[\s-]type\b|"
    r"dupe|clone|alternative|"
    r"decant|sample|atomi[sz]er|"
    r"gift\s+set|travel\s+size|mini(?:\s+set)?|"
    r"refill|tester|"
    r"body\s+wash|shower\s+gel|lotion|deodorant|aftershave|balm|"
    r"candle|wax\s+melt|air\s+freshener|"
    r"t[\s-]?shirt|hoodie|mug|poster|"
 

    r"creations?\s+of|" 
    r"\bm\s*&\s*h\b|musk\s*&\s*hustle|"
    r"\d+\s*(in\s*\d+|original|pack|bundle|popular)|"
    r"set\s+of\s+\d+|\d+\s+piece\s+set|"
    r"variety\s+pack|sampler\s+set|discovery\s+set|"
    r"comparable\s+to|similar\s+to|matches\s+the\s+scent|"
    r"pure\s+oil\s+cologne|oil\s+cologne|"
    r"fragrance\s+dupe|perfume\s+dupe"
    r")\b",
    re.IGNORECASE,
)

FRAG_BUNDLE_MARKERS = re.compile(
    r"\baventus\b|\berolfa\b|\bimagination\b|\boud\s+wood\b|"
    r"\btobacco\s+vanille\b|\bbaccarat\b|\bgreen\s+irish\s+tweed\b|"
    r"\bsilver\s+mountain\b|\bmillesime\b|\bviking\b|"
    r"\bneroli\s+savage\b|\bspice\s+\&?\s*wood\b",
    re.IGNORECASE,
)

STOPWORDS = {
    "the", "by", "for", "of", "and", "&", "eau", "de", "parfum",
    "toilette", "cologne", "edp", "edt", "men", "women", "mens", "womens",
    "spray", "perfume", "ml", "oz", "fl",
}

def _is_bundle(card_text):
    """True if the card mentions 3+ distinct fragrance line names —
    a strong signal that it's a multi-product dupe bundle, not a
    single-fragrance listing."""
    hits = set(m.group(0).lower() for m in FRAG_BUNDLE_MARKERS.finditer(card_text))
    return len(hits) >= 3


def clean_price_text(text):
    if not text:
        return text
    return text.replace("\xa0", " ").replace("Â", "").strip()



def oz_to_ml(oz):
    return oz * 29.5735


def extract_oz(text):
    m = re.search(r"(\d+(\.\d+)?)\s*oz", text.lower())
    return float(m.group(1)) if m else None

def extract_ml(text):
    text = text.lower()

    m = re.search(r"(\d+(?:\.\d+)?)\s*ml", text)
    if m:
        return float(m.group(1))

    m = re.search(r"(\d+(?:\.\d+)?)\s*(?:fl\s*)?oz", text)
    if m:
        return float(m.group(1)) * 29.5735

    return None

_PRICE_PER_UNIT_RE = re.compile(
    r"\$\s*\d+(?:\.\d+)?\s*/\s*(?:fl\s*\.?\s*)?(?:oz|ounce)",
    re.IGNORECASE,
)
 
def _strip_price_per_unit(text):
    """Remove '$X.XX/fluid ounce' style fragments so they don't pollute size."""
    return _PRICE_PER_UNIT_RE.sub("", text)

def extract_all_ml(text):
    text = text.lower()
    values = []

    for m in re.findall(r"(\d+(?:\.\d+)?)\s*ml", text):
        values.append(float(m))

    for m in re.findall(r"(\d+(?:\.\d+)?)\s*(?:fl\s*)?oz", text):
        values.append(float(m) * 29.5735)

    return values

def extract_all_ml_from_card(card):
    """
    Extract all size values mentioned in the *primary* visible parts of a
    card — the h2 title and the size-badge span — NOT the options dropdown
    or the price-per-unit text.
 
    This is what we use for size scoring. Returns a list of ml values.
    """
    text_parts = []

    h2 = card.select_one("h2")
    if h2:
        text_parts.append(h2.get_text(" ", strip=True))
 
    for sel in (
        "span.s-background-color-platinum",
        'span[class*="size-badge"]',
        'div[data-cy="title-recipe"] span',
    ):
        for el in card.select(sel):
            t = el.get_text(" ", strip=True)
            if re.search(r"\d+\s*(?:ml|fl\s*\.?\s*oz|oz|ounce)", t, re.I):
                text_parts.append(t)
 
    combined = " ".join(text_parts)
    combined = _strip_price_per_unit(combined)
    return extract_all_ml(combined) 

def size_check(text_or_values, target_ml):
    """Three-state size check. Accepts either:
      - a string (parsed with extract_all_ml internally), or
      - a list of pre-extracted ml values
 
    Returns "match" / "conflict" / "unknown".
    """
    if isinstance(text_or_values, str):
        values = extract_all_ml(text_or_values)
    else:
        values = list(text_or_values or [])
 
    if not values:
        return "unknown"
 
    for v in values:
        if abs(v - target_ml) / target_ml <= 0.12:
            return "match"
    return "conflict"

def size_matches(title, target_ml):
    """Backward-compatible wrapper — True for match or unknown, False only
    for explicit conflict."""
    return size_check(title, target_ml) != "conflict"


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


def build_product_struct(product, query):

    name = product["name"].lower().replace("'", "").replace("\u2019", "")
    tokens = re.findall(r"[a-z]+", name)
    name_tokens = {t for t in tokens if t not in STOPWORDS and len(t) > 1}

    size_ml = product.get("size_ml") or product.get("bottle_ml")
    if size_ml is None and product.get("size"):
        m = re.search(r"(\d+)\s*ml", str(product["size"]).lower())
        if m:
            size_ml = int(m.group(1))
    if size_ml is None:
        m = re.search(r"(\d+)\s*ml", query.lower())
        if m:
            size_ml = int(m.group(1))

    size_alternatives = product.get("size_alternatives", [])
    allowed_sizes = [size_ml] if size_ml else []
    allowed_sizes.extend(size_alternatives)

    return {
        "brand":         product["brand"].lower(),
        "name_tokens":   name_tokens,
        "variant":       extract_variant(query),
        "size_ml":       size_ml,
        "allowed_sizes": [s for s in allowed_sizes if s],
        "size_strict":   product.get("size_strict", True),
    }

def normalize_text(text):
    text = text.lower()
    text = text.replace("’", "'")
    text = text.replace("'", "")
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    return text


def score_match(title, product_struct, card_text="", product=None, card=None):
    """Card scoring. New `card` parameter is the BeautifulSoup element so
    we can extract sizes from the title+badge specifically rather than the
    full flattened card text (which includes dropdown options).
    """
    title_n = normalize_text(title)
    full_n  = normalize_text(card_text or title)
 
    score = 0
    title_tokens   = set(title_n.split())
    product_tokens = product_struct["name_tokens"]

    if NON_FRAG_RE.search(full_n):
        return -999
    if _is_bundle(full_n):
        return -999
 

    brand = product_struct["brand"]
    if brand not in full_n:
        return -999
    elif brand == "dior" and "christian dior" in full_n:
        score += 3
    elif brand != "dior":
        score += 1
 
    if re.search(r"\b(sample|vial|mini|decant)\b", full_n):
        return -999

    if re.search(r"\b(0\.1|0\.17|0\.2|5\s*ml|10\s*ml)\b", full_n):
        score -= 8

    overlap = product_tokens.intersection(title_tokens)
    score += len(overlap)
 
    if product_tokens:
        coverage = len(overlap) / float(len(product_tokens))
        if coverage < 0.45:
            return -999
 
    if product:
        full_name = normalize_text(product["name"])
        if full_name in full_n:
            score += 8
 
    title_sizes = extract_all_ml_from_card(card) if card is not None else []
 
    if product_struct["allowed_sizes"]:
        if title_sizes:
            results = [size_check(title_sizes, s)
                       for s in product_struct["allowed_sizes"]]
            if "match" in results:
                score += 6
            elif all(r == "conflict" for r in results):
                score -= 6
            else:
                score -= 2
        else:
            score -= 2

    if title_sizes and product_struct["size_ml"]:
        closest = min(title_sizes,
                      key=lambda v: abs(v - product_struct["size_ml"]))
        diff = abs(closest - product_struct["size_ml"]) / product_struct["size_ml"]
        if diff < 0.05:
            score += 4
        elif diff < 0.12:
            score += 2
        else:
            score -= 3
 
    if product_struct["variant"] and product_struct["variant"] in full_n:
        score += 1
 
    return score


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


_PRICE_RE = re.compile(r"^\s*(?:AED|US\$|\$|£|€)\s*\d[\d,\.]*\s*$")


def _clean(s):
    """Collapse whitespace and strip NBSP-ish junk from a price string."""
    if not s:
        return None
    return s.replace("\xa0", " ").replace("Â", "").strip()

def extract_price(card):
    """
    Return the correct card price, preferring the featured offer over
    'More Buying Choices'.

    Resolution order:
        1. div[data-cy="price-recipe"]  → span.a-price span.a-offscreen
        2. div[data-cy="secondary-offer-recipe"]  → first a-color-base span
        3. Any span.a-offscreen in the card that matches a price pattern
    """

    primary = card.select_one(
        'div[data-cy="price-recipe"] span.a-price span.a-offscreen'
    )
    if primary:
        txt = _clean(primary.get_text(strip=True))
        if txt and _PRICE_RE.match(txt):
            return txt

    if not card.select_one('div[data-cy="secondary-offer-recipe"]'):
        any_primary = card.select_one("span.a-price span.a-offscreen")
        if any_primary:
            txt = _clean(any_primary.get_text(strip=True))
            if txt and _PRICE_RE.match(txt):
                return txt

    secondary = card.select_one('div[data-cy="secondary-offer-recipe"]')
    if secondary:
        for span in secondary.select("span.a-color-base"):
            txt = _clean(span.get_text(strip=True))
            if txt and _PRICE_RE.match(txt):
                return txt

    for el in card.select("span.a-offscreen"):
        txt = _clean(el.get_text(strip=True))
        if txt and _PRICE_RE.match(txt) and "/" not in txt:
            return txt

    return None

def normalize_price(price):
    if not price:
        return None

    price = clean_price_text(price)
    logger.info(f"Extracted price: {price}")

    m = re.search(r"(AED|\$)\s?(\d[\d,\.]*)", price)
    return f"{m.group(1)}{m.group(2)}" if m else price


def scrape_one(page, query, product, site_key, scrape_date):
    """
    Search Amazon and return the best-matching card.
 
    Changes from the previous version:
 
    * USA search URL strips the size suffix entirely. Amazon's search ranker
      responds very differently to "Creed Aventus" vs "Creed Aventus 3.4 oz";
      the latter ranks dupes higher because they are more likely to literally
      contain "3.4 oz" in their title than Creed's actual 3.38 fl oz listing.
    * The fallback path also now runs NON_FRAG_RE and the full-token check,
      so dupes can't sneak in through the loose fallback either.
    * The scan window is 15 cards for the strong path and 8 for the fallback
      (up from 5) so legitimate Creed listings that sit below 4-5 dupe ads
      still get considered.
    """
    search_query = query.strip()
 
    if site_key == "USA":
        search_query = re.sub(
            r"\s*\d+(?:\.\d+)?\s*(?:ml|fl\s*\.?\s*oz|oz|ounce)s?\b",
            "",
            search_query,
            flags=re.I,
        ).strip()
 
    url = f"{SITES[site_key]['domain']}/s?k={search_query.replace(' ', '+')}"
 
    html = _fetch(page, url)
    if not html:
        return None
 
    soup = BeautifulSoup(html, "html.parser")
    cards = soup.select('[data-component-type="s-search-result"]')
 
    product_struct = build_product_struct(product, query)

    best_card = None
    best_score = -999
    target_ml = product_struct.get("size_ml")
 
    for card in cards[:60]:
        title_el = card.select_one("h2")
        if not title_el:
            continue
 
        title = title_el.get_text(" ", strip=True)
        card_text = card.get_text(" ", strip=True)
 
        s = score_match(title, product_struct, card_text, product, card)
        logger.info(f"SCORE: {s} | {title}")
 
        if s > best_score:
            best_score = s
            best_card = card
        elif s == best_score and best_card is not None and target_ml:
            new_sizes  = extract_all_ml_from_card(card)
            best_sizes = extract_all_ml_from_card(best_card)
 
            def _closest_diff(sizes):
                if not sizes:
                    return float("inf")
                return min(abs(v - target_ml) for v in sizes)
 
            if _closest_diff(new_sizes) < _closest_diff(best_sizes):
                best_card = card

    if best_score < 2:
        logger.info(f"  no confident match for: {product['brand']} {product['name']}")
        return None
 
    if not best_card:
        logger.info(f"  no match for: {product['brand']} {product['name']}")
        return None
 
    return extract_data(best_card, product, site_key, scrape_date, url)


def _extract_rating_text(card):
    """Pull the 'X out of 5 stars' text from a search result card."""
    el = card.select_one("i.a-icon-star-small span.a-icon-alt") \
         or card.select_one("span.a-icon-alt") \
         or card.select_one("i[class*='a-star'] span")
    if el:
        txt = el.get_text(strip=True)
        if "out of" in txt.lower():
            return txt
    return None


def _extract_review_count(card):
    """Pull the review count (e.g. '1,234') from a search result card."""
    for sel in (
        "a[href*='#customerReviews'] span.a-size-base",
        "span.a-size-base.s-underline-text",
        "a.a-link-normal span.a-size-base",
    ):
        el = card.select_one(sel)
        if el:
            txt = el.get_text(strip=True)
            if re.match(r"^[\d,]+$", txt):
                return txt
    return None


def _size_from_product_or_title(product, title):
    """Prefer the size in products.json; fall back to parsing the card title."""
    sz = product.get("size")
    if sz:
        return str(sz)
    if title:
        m = re.search(r"(\d+(?:\.\d+)?\s*(?:ml|fl\s*oz|oz))", title, re.I)
        if m:
            return m.group(1)
    return None


def extract_data(card, product, site_key, scrape_date, url):
    title_el = card.select_one("h2 span")
    title = title_el.get_text(strip=True) if title_el else None
    price = normalize_price(extract_price(card))

    return {
        "scrape_date":        scrape_date,
        "country":            site_key,
        "currency":           SITES[site_key]["currency"],
        "name":               product["name"],
        "brand":              product["brand"],
        "category":           product.get("category", "fragrance"),
        "price_raw":          price,
        "bottle_size_raw":    _size_from_product_or_title(product, title),
        "amazon_rating_raw":  _extract_rating_text(card),
        "amazon_reviews_raw": _extract_review_count(card),
        "amazon_title_raw":   title,
        "amazon_url":         url,
        "availability":       "in_stock" if price else "unavailable",
    }

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


def save_raw(data, path="raw_data/amazon_raw.csv"):
    """Persist Amazon scrape results, deduplicating same-day re-runs.
 
    Behaviour:
    - Existing rows from earlier dates are preserved.
    - Existing rows from the same date for the same product+country
      are REPLACED by the fresh row, not appended alongside.
    - First run on an empty path just writes the new data.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
 
    new_df = pd.DataFrame(data)

    key_cols = ["scrape_date", "country", "name", "brand"]
 
    if os.path.exists(path):
        try:
            existing = pd.read_csv(path, encoding="utf-8-sig")
        except Exception as e:
            logger.warning(f"Could not read existing {path}: {e} — starting fresh")
            existing = pd.DataFrame()
    else:
        existing = pd.DataFrame()
 
    if not existing.empty and set(key_cols).issubset(existing.columns):

        new_keys = set(
            tuple(row[c] for c in key_cols)
            for _, row in new_df.iterrows()
        )
 
        before = len(existing)
        existing = existing[
            ~existing[key_cols].apply(tuple, axis=1).isin(new_keys)
        ]
        replaced = before - len(existing)
        if replaced:
            logger.info(f"Replaced {replaced} existing rows with fresh data")
 
    combined = pd.concat([existing, new_df], ignore_index=True)
 
    try:
        combined.to_csv(path, index=False, encoding="utf-8-sig")
    except PermissionError:
        alt_path = path.replace(".csv", "_backup.csv")
        logger.warning(f"File locked. Saving to {alt_path}")
        combined.to_csv(alt_path, index=False, encoding="utf-8-sig")
        path = alt_path
 
    logger.info(f"Saved {len(new_df)} new rows ({len(combined)} total in {path})")
    return new_df


if __name__ == "__main__":
    with open("products.json") as f:
        products = json.load(f)

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    data = scrape_all(products, today, limit=3)

    if data:
        df = save_raw(data)
        print(df[["name", "country", "price_raw"]])