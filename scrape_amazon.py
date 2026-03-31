"""
scrape_amazon.py — Scrapes Amazon UK + UAE prices using Playwright + stealth
=============================================================================
Uses playwright-stealth to hide headless Chromium fingerprints that Amazon's
bot detection checks for (navigator.webdriver, missing plugins, etc.)

Install:
    pip install playwright playwright-stealth
    playwright install chromium
"""

import re, time, random, json, os, logging
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("amazon")

SITES = {
    "USA": {"base_url": "https://www.amazon.com/s?k=", "currency": "USD"},
    "UAE": {"base_url": "https://www.amazon.ae/s?k=",  "currency": "AED"},
}

# Amazon USA lists fragrances in fl oz — map common ml sizes to their US label
ML_TO_FLOZ = {
    10:  "0.33",
    15:  "0.5",
    20:  "0.67",
    30:  "1.0",
    40:  "1.35",
    50:  "1.7",
    60:  "2.0",
    75:  "2.5",
    80:  "2.7",
    90:  "3.0",
    100: "3.4",
    125: "4.2",
    150: "5.0",
    200: "6.7",
    250: "8.4",
}

def _usa_query(query: str) -> str:
    """Replace 'NNml' with fl oz equivalent for Amazon USA searches."""
    import re
    def _replace(m):
        ml = int(m.group(1))
        floz = ML_TO_FLOZ.get(ml)
        return f"{floz} fl oz" if floz else m.group(0)
    return re.sub(r'(\d+)\s*ml', _replace, query, flags=re.IGNORECASE)

MIN_DELAY = 5.0
MAX_DELAY = 10.0


# Playwright helpers

def _start_browser():
    """Launch Playwright + Chromium. Returns (playwright, browser)."""
    from playwright.sync_api import sync_playwright
    pw      = sync_playwright().start()
    browser = pw.chromium.launch(
        headless=True,
        args=[
            "--disable-blink-features=AutomationControlled",
            "--no-sandbox",
            "--disable-dev-shm-usage",
        ],
    )
    return pw, browser


def _apply_stealth(page):
    """Apply stealth patches using the installed playwright-stealth API."""
    try:
        from playwright_stealth import stealth
        stealth(page)
    except Exception:
        pass  # stealth unavailable — continue without it


def _new_page(browser, site_key="USA"):
    """Fresh context + page with stealth patches applied."""
    context = browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        viewport={"width": 1280, "height": 800},
        locale="en-US",
        extra_http_headers={
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        },
    )
    # Force USD on amazon.com — overrides geo-based EUR redirect for EU IPs
    if site_key == "USA":
        context.add_cookies([{
            "name":   "i18n-prefs",
            "value":  "USD",
            "domain": ".amazon.com",
            "path":   "/",
        }])
    page = context.new_page()
    _apply_stealth(page)
    return context, page


def _fetch(browser, url, site_key="USA"):
    """Fetch url with a fresh stealthy Playwright context."""
    context, page = _new_page(browser, site_key)
    try:
        page.goto(url, timeout=25000, wait_until="domcontentloaded")
        try:
            page.wait_for_selector('[data-component-type="s-search-result"]', timeout=8000)
        except Exception:
            pass
        return page.content()
    except Exception as exc:
        logger.debug(f"  Fetch error {url}: {exc}")
        return None
    finally:
        try:
            page.close()
            context.close()
        except Exception:
            pass


# Per-product scrape

def scrape_one(browser, query, site_key, scrape_date):
    """Search Amazon and extract data from the first result."""
    site = SITES[site_key]

    # ── USA: strip ml from query (don't convert to fl oz in the URL).
    # Converting "100ml → 3.4 fl oz" causes Amazon to rank unrelated 3.4 oz products
    # (e.g. Sauvage EDT) above the intended fragrance when name-only ranking is better.
    # Instead we derive the fl oz target size for tiebreaking only.
    if site_key == "USA":
        _ml_match = re.search(r'(\d+)\s*ml', query, re.I)
        if _ml_match:
            _ml_val = int(_ml_match.group(1))
            _size_target = ML_TO_FLOZ.get(_ml_val)   # e.g. "3.4" for 100ml
        else:
            _fl_match = re.search(r'(\d+(?:\.\d+)?)\s*fl\s*oz', query, re.I)
            _size_target = _fl_match.group(1) if _fl_match else None
        # Strip any ml / fl oz size from the search query — search by name only
        query_clean = re.sub(r'\d+(?:\.\d+)?\s*(?:ml|fl\s*oz|oz)\b', '', query, flags=re.I).strip()
    else:
        query_clean = query
        _size_target = None

    url = site["base_url"] + query_clean.replace(" ", "+")
    if site_key == "USA":
        url += "&currency=USD"

    empty = {
        "scrape_date":        scrape_date,
        "country":            site_key,
        "currency":           site["currency"],
        "price_raw":          None,
        "bottle_size_raw":    None,
        "amazon_rating_raw":  None,
        "amazon_reviews_raw": None,
        "availability":       "not_found",
        "amazon_title_raw":   None,
        "amazon_url":         url,
    }

    html = _fetch(browser, url, site_key)
    if not html:
        empty["availability"] = "fetch_failed"
        return empty

    if "captcha" in html.lower() or "robot check" in html.lower():
        logger.warning(f"  {site_key}: CAPTCHA detected")
        empty["availability"] = "captcha_blocked"
        return empty

    soup  = BeautifulSoup(html, "html.parser")
    cards = soup.select('[data-component-type="s-search-result"]')

    if not cards:
        logger.debug(f"  {site_key}: no result cards found")
        return empty

    import re as _re

    # ─Pick best-matching card rather than blindly using cards[0]
    # Sponsored results and related products often appear before the target.
    # Score each card by how many query tokens appear in its title.
    def _score_card(card, query_tokens):
        title_el = (
            card.select_one("h2[aria-label] span")     # product title h2 always has aria-label
            or card.select_one("h2 a span")
            or card.select_one("h2 span.a-text-normal")
            or card.select_one('[data-cy="title-recipe"] h2 span')
            or card.select_one("h2 span")
        )
        if not title_el:
            return 0, ""
        # Score against ALL h2 text in card (product title + brand label)
        # so "Dior" in a separate brand h2 still contributes to the score
        full_h2 = " ".join(el.get_text(strip=True).lower() for el in card.select("h2"))
        title_lower = title_el.get_text(strip=True).lower()
        score = sum(1 for t in query_tokens if t in full_h2)
        return score, title_lower

    # Tokenise query: strip embedded size units first so "60ml" → "" not a whole token,
    # then drop stop-words and bare digits.
    _stop = {"by", "for", "men", "women", "oz", "fl", "spray", "pack", "of", "de", "ml"}
    query_clean_tok = _re.sub(r'\d+(?:\.\d+)?\s*(?:ml|fl\s*oz|oz)\b', '', query.lower())
    query_tokens = [
        w for w in _re.sub(r'[^\w\s]', '', query_clean_tok).split()
        if w not in _stop and not w.isdigit()
    ]

    # Size target for tiebreaking:
    # USA → use _size_target derived from ML_TO_FLOZ (set in scrape_one before URL build)
    # UAE → parse fl oz from query if present
    if site_key == "USA":
        size_str = _size_target        # e.g. "3.4" for 100ml — used to prefer correct size card
    else:
        _size_match = _re.search(r'(\d+(?:\.\d+)?)\s*fl\s*oz', query.lower())
        size_str = _size_match.group(1) if _size_match else None

    # Stricter threshold: 85% of meaningful tokens must match.
    # 4 tokens → need 4; 3 tokens → need 3; 2 tokens → need 2.
    # This rejects Elixir (3/4) for an EDP query, and fragrance-oil dupes (2/3) for Oud Ispahan.
    import math as _math
    min_score = max(2, _math.ceil(len(query_tokens) * 0.85))

    # Brand check: if "dior" (or any single brand token) is in the query, the card title
    # must contain it — rejects "dark oud ispahan" perfume oils, "1000 fahrenheit" oils etc.
    brand_tokens = [t for t in query_tokens if t in ("dior", "chanel", "creed", "armani",
                    "versace", "gucci", "burberry", "cartier", "hermes", "givenchy",
                    "lancome", "prada", "dolce", "gabbana", "bvlgari", "amouage",
                    "maison", "margiela", "parfums", "kilian", "lattafa", "ajmal",
                    "rasasi", "swiss", "arabian", "zara")]

    # Non-fragrance blacklist: skip aftershave, lotions, shower gels etc.
    NON_FRAGRANCE_RE = _re.compile(
        r'\b(after\s*shave|aftershave|lotion|body\s*wash|shower\s*gel|'
        r'deodorant|deo\b|balm|shampoo|conditioner|soap|cream|gel|'
        r'travel\s*set|gift\s*set)\b',
        _re.I
    )

    best_card   = None
    best_score  = -1
    best_size   = False   # does best card match the fl oz size?
    best_title  = ""
    for c in cards[:20]:   # check top 20 results
        s, t = _score_card(c, query_tokens)
        # Skip non-fragrance product types regardless of score
        if NON_FRAGRANCE_RE.search(t):
            logger.debug(f"  {site_key}: skipping non-fragrance card: \"{t[:60]}\"")
            continue
        # Brand must appear somewhere in the card h2 text (title OR brand label)
        card_h2_text = " ".join(el.get_text(strip=True).lower() for el in c.select("h2"))
        if brand_tokens and not any(b in card_h2_text for b in brand_tokens):
            logger.debug(f"  {site_key}: skipping off-brand card: \"{t[:60]}\"")
            continue
        # Fragrance sub-type exclusion: if the card title contains a distinguishing
        # variant word (elixir, intense, noir…) that is NOT in the query, skip it.
        # Prevents "Sauvage Parfum" query from matching "Sauvage Elixir Parfum Concentre".
        SUBTYPES = {"elixir", "intense", "noir", "extreme", "blanche", "extrait",
                    "midnight", "absolu", "prive", "exclusive"}
        card_subtypes = SUBTYPES & set(t.split())
        query_subtypes = SUBTYPES & set(query_tokens)
        if card_subtypes - query_subtypes:   # card has subtype words not in query
            logger.debug(f"  {site_key}: skipping subtype mismatch card: \"{t[:60]}\"")
            continue
        # Size tiebreaker + hard filter
        # Check BOTH the size badge AND the title for size mentions
        card_text = t
        size_badge = c.select_one('[data-csa-c-content-id="alf-size-badge-component"]')
        badge_text = size_badge.get_text(strip=True).lower() if size_badge else ""
        if badge_text:
            card_text += " " + badge_text
        card_has_size = bool(size_str and size_str in card_text)

        # Hard size filter: reject cards with a clearly different size.
        # Matches both "fl oz" and "ounce(s)" in badge OR title.
        # Uses numeric tolerance ±0.25 oz to handle "2.0" vs "2.03" rounding.
        if size_str:
            SIZE_PAT = _re.compile(
                r'(\d+(?:\.\d+)?)\s*(?:fl\s*oz|fluid\s*oz|ounces?)\b', _re.I
            )
            # Check badge first, then fall back to title
            check_text = badge_text if badge_text else t
            size_hit = SIZE_PAT.search(check_text)
            if size_hit:
                try:
                    found_fl = float(size_hit.group(1))
                    target_fl = float(size_str)
                    if abs(found_fl - target_fl) > 0.25:
                        logger.debug(
                            f"  {site_key}: skipping wrong-size card "
                            f"(want {size_str}oz, found {found_fl}oz): \"{t[:50]}\""
                        )
                        continue
                except (ValueError, TypeError):
                    pass

        # Prefer higher score; break ties by size match
        if s > best_score or (s == best_score and card_has_size and not best_size):
            best_score = s
            best_card  = c
            best_title = t
            best_size  = card_has_size

    matched_title = best_title[:80] if best_title else "—"
    logger.info(
        f"  {site_key}: card match score={best_score}/{len(query_tokens)} "
        f"(min={min_score}) → \"{matched_title}\""
    )

    # If no card meets the threshold the product isn't on this marketplace
    if best_score < min_score or best_card is None:
        logger.info(f"  {site_key}: no confident match — marking unavailable")
        empty["availability"] = "not_on_marketplace"
        return empty

    card = best_card
    # Try progressively broader title selectors — product title h2 always has aria-label
    title_el = (
        card.select_one("h2[aria-label] span")
        or card.select_one("h2 a span")
        or card.select_one("h2 span.a-text-normal")
        or card.select_one('[data-cy="title-recipe"] h2 span')
        or card.select_one("h2 span")
    )
    rating_el  = card.select_one('[aria-label*="out of 5 stars"]')
    reviews_el = card.select_one(".a-size-base.s-underline-text")

    expected_currency = site["currency"]  # "USD" or "AED"
    currency_symbols  = {"USD": "$", "AED": "AED"}
    sym = currency_symbols.get(expected_currency, "")

    # amazon.ae geo-redirects EU visitors to EUR pricing, so we also accept EUR/€
    # as a valid price signal and record whatever currency was actually served.
    ANY_CURRENCY_RE = _re.compile(
        r'(?:£|\$|€|EUR|GBP|AED|USD)\s*[\d,.]|[\d,.]+\s*(?:EUR|GBP|AED|USD)',
        _re.I
    )

    # ── Price extraction — 4 fallbacks ───────────────────────────────────────
    price_text      = None
    detected_currency = expected_currency  # will be overridden if EUR served

    def _looks_like_price(t, sym):
        """Accept expected currency OR any recognisable currency symbol/code."""
        if sym and _re.search(rf'(?:{_re.escape(sym)}|{expected_currency})\s*[\d,.]', t, _re.I):
            return True
        return bool(ANY_CURRENCY_RE.search(t))

    def _extract_currency(t):
        """Return the currency code detected in a price string."""
        for code in ("GBP", "AED", "USD", "EUR"):
            if code in t.upper():
                return code
        if "£" in t:
            return "GBP"
        if "€" in t:
            return "EUR"
        if "$" in t:
            return "USD"
        return expected_currency

    # 1. data-csa-c-price-to-pay attribute (most reliable — numeric, no encoding)
    atc = card.select_one("[data-csa-c-price-to-pay]")
    if atc:
        numeric = atc.get("data-csa-c-price-to-pay", "").strip()
        if numeric:
            price_text = f"{sym}{numeric}"
            detected_currency = expected_currency

    # 2. Featured offer Buy Box: data-cy="price-recipe" .a-offscreen
    if not price_text:
        recipe = card.select_one('[data-cy="price-recipe"]')
        if recipe:
            el = recipe.select_one(".a-price .a-offscreen")
            if el:
                t = el.get_text(strip=True)
                if _looks_like_price(t, sym):
                    price_text = t
                    detected_currency = _extract_currency(t)

    # 3. Any .a-price .a-offscreen in the card
    if not price_text:
        for el in card.select(".a-price .a-offscreen"):
            t = el.get_text(strip=True)
            if _looks_like_price(t, sym):
                price_text = t
                detected_currency = _extract_currency(t)
                break

    # 4. Secondary offer row: "No featured offers available EUR 114.55 (11 new offers)"
    #    amazon.ae geo-serves EUR to EU IPs — this is where our prices land.
    if not price_text:
        secondary = card.select_one('[data-cy="secondary-offer-recipe"]')
        if secondary:
            for span in secondary.find_all("span"):
                t = span.get_text(strip=True).replace("\xa0", " ")  # normalise NBSP
                if _looks_like_price(t, sym):
                    price_text = t
                    detected_currency = _extract_currency(t)
                    break

    return {
        "scrape_date":        scrape_date,
        "country":            site_key,
        "currency":           detected_currency,   # actual currency served (may differ from expected)
        "price_raw":          price_text,
        "bottle_size_raw":    title_el.get_text(strip=True)[:200] if title_el else None,
        "amazon_rating_raw":  rating_el.get("aria-label") if rating_el else None,
        "amazon_reviews_raw": reviews_el.get_text(strip=True) if reviews_el else None,
        "availability":       "in_stock" if price_text else "unavailable",
        "amazon_title_raw":   title_el.get_text(strip=True)[:200] if title_el else None,
        "amazon_url":         url,
    }


# Batch scraping

def scrape_all(products, scrape_date, limit=None):
    """Scrape Amazon UK + UAE for all products."""
    prods   = products[:limit] if limit else products
    results = []

    pw, browser = _start_browser()
    try:
        for i, p in enumerate(prods):
            logger.info(f"[{i+1}/{len(prods)}] Amazon: {p['brand']} — {p['name']}")
            query = p.get("amazon_search", f"{p['brand']} {p['name']}")

            for country in ["USA", "UAE"]:
                row = scrape_one(browser, query, country, scrape_date)
                row["name"]     = p["name"]
                row["brand"]    = p["brand"]
                row["category"] = p.get("category", "")
                results.append(row)
                logger.info(
                    f"  {country}: price={row['price_raw']} | "
                    f"avail={row['availability']} | "
                    f"rating={row['amazon_rating_raw']}"
                )
                time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))

    finally:
        browser.close()
        pw.stop()

    logger.info(f"Amazon done: {len(results)} records ({len(prods)} products × 2 countries)")
    return results


# Persistence

def save_raw(data, path="raw_data/amazon_raw.csv"):
    """Append scraped data to CSV."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df     = pd.DataFrame(data)
    exists = os.path.exists(path)
    df.to_csv(path, mode="a", header=not exists, index=False, encoding="utf-8-sig")
    logger.info(f"Saved {len(df)} rows → {path}")
    return df


if __name__ == "__main__":
    with open("products.json") as f:
        products = json.load(f)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    data  = scrape_all(products, today, limit=3)
    if data:
        df = save_raw(data)
        print(df[["name", "country", "price_raw", "availability"]].to_string())
