"""
scrape_fragrantica.py — Scrapes Fragrantica using Playwright (headless Chrome)
===============================================================================
Uses Playwright to render JavaScript so Vue components fully hydrate before
BeautifulSoup parses the page.  This is required because vote data (love/like/
ok/dislike/hate, seasons, wear-time) is injected by the likes-rating-new and
seasons-rating-new Vue components — they never appear in the raw server HTML.

All other data (rating, notes, accords, perfumers, year, family, gender) was
already working with cloudscraper and is unchanged here.


DOM notes
---------
 * Rating card / When-To-Wear card:
     Each card is  div.tw-rating-card
     The card title is  span.tw-rating-card-label
     Each data block is  div.flex-col.items-center  (CSS multi-class selector)
     Label text = first direct-child <span> whose text matches a target word
     Vote count  = span.tabular-nums  anywhere inside that same block

   KEY: BeautifulSoup's class_ lambda is called ONE class name at a time,
   so AND-conditions like "flex-col AND items-center" cannot be written with it.
   Use  element.select("div.flex-col.items-center")  (CSS selector) instead.

 * Note pyramid — 3  div.pyramid-level-container  in order: top, middle, base
 * Main accords — span.truncate inside the "main accords" h6 sibling div
 * Perfumers    — <a href="/noses/..."> link text spans
 * Year / family / gender — regex on  #perfume-description-content  text
"""

import re
import time
import random
import json
import os
import logging
import pandas as pd
from bs4 import BeautifulSoup, Tag
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("fragrantica")

# Polite-crawl delays
MIN_DELAY   = 5.0
MAX_DELAY   = 10.0
PAUSE_EVERY = 20
PAUSE_MIN   = 30
PAUSE_MAX   = 60

# Playwright page fetch

def _new_page(browser):
    """Create a fresh browser context + page (resets cookies/session state)."""
    context = browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        viewport={"width": 1280, "height": 800},
        locale="en-US",
    )
    page = context.new_page()
    return context, page


def _page_is_blocked(html):
    """Return True if the page looks like a Cloudflare challenge or empty page."""
    if len(html) < 5000:
        return True
    # No description content = not a real perfume page
    if 'id="perfume-description-content"' not in html and "perfume-description-content" not in html:
        return True
    return False


def _fetch_rendered_html(page, url, timeout_ms=30000):
    """
    Navigate to url and return fully-rendered HTML after Vue components mount.
    Waits for .tw-rating-card div.flex-col.items-center (the hydrated vote card).
    Falls back to a flat 5-second wait if the selector never appears.
    """
    page.goto(url, timeout=timeout_ms, wait_until="domcontentloaded")

    # Wait for Vue to hydrate the rating card — up to 12 s
    try:
        page.wait_for_selector(
            ".tw-rating-card div.flex-col.items-center",
            timeout=12000,
        )
    except Exception:
        page.wait_for_timeout(5000)

    return page.content()


def create_browser_context():
    """
    Launch Playwright + Chromium browser.
    Returns (playwright, browser).

    Usage:
        pw, browser = create_browser_context()
        try:
            ...
        finally:
            browser.close()
            pw.stop()
    """
    from playwright.sync_api import sync_playwright
    pw      = sync_playwright().start()
    browser = pw.chromium.launch(headless=True)
    return pw, browser


# Vote-count string → integer

def _parse_vote_count(text):
    """
    Convert a Fragrantica display count to int.

    "10.5k" → 10500 | "8k" → 8000 | "968" → 968 | None → None
    """
    if not text:
        return None
    t = str(text).strip().lower().replace(",", "")
    if "k" in t:
        try:
            return int(float(t.replace("k", "")) * 1000)
        except (ValueError, TypeError):
            return None
    try:
        return int(float(t))
    except (ValueError, TypeError):
        return None


# Generic card-vote extractor

def _extract_card_votes(soup, card_header_text, target_labels):
    """
    Extract label → vote-count pairs from a Fragrantica rating card.

    Fragrantica has two rating cards:
        "Rating"       — love / like / ok / dislike / hate
        "When To Wear" — winter / spring / summer / fall / day / night

    Each card contains  div.flex-col.items-center  blocks.  Each block has:
        div (icon)
        span "love"            ← direct-child label span
        div.w-full
          div (progress bar)
          span.tabular-nums    ← vote count

    Parameters
    ----------
    soup             : BeautifulSoup of the full page
    card_header_text : "Rating" or "When To Wear"
    target_labels    : set of lowercase strings to match (e.g. {"love","like"})

    Returns
    -------
    dict  {label: int_count}
    """
    results = {}

    # Find the correct card
    card = None
    for candidate in soup.find_all(class_="tw-rating-card"):
        lbl_el = candidate.find(class_="tw-rating-card-label")
        if lbl_el and card_header_text.lower() in lbl_el.get_text(strip=True).lower():
            card = candidate
            break

    if card is None:
        logger.debug(f"  Card '{card_header_text}' not found in page")
        return results


    for block in card.select("div.flex-col.items-center"):
        # First direct-child <span> whose text matches a target label
        label_text = None
        for child in block.children:
            if isinstance(child, Tag) and child.name == "span":
                txt = child.get_text(strip=True).lower()
                if txt in target_labels:
                    label_text = txt
                    break

        if label_text is None:
            continue

        # Vote count = span.tabular-nums anywhere inside this block
        count_span = block.find("span", class_="tabular-nums")
        if count_span:
            results[label_text] = _parse_vote_count(count_span.get_text(strip=True))

    return results


# Notes extraction

def _extract_notes_from_pyramid(soup):
    """
    Extract top / middle / base notes from the three
    div.pyramid-level-container elements (in order: top, middle, base).
    """
    containers = soup.find_all("div", class_="pyramid-level-container")
    top_notes = mid_notes = base_notes = None

    for i, container in enumerate(containers):
        labels = container.find_all(
            "span",
            class_=lambda c: c and "pyramid-note-label" in c,
        )
        notes = [lb.get_text(strip=True) for lb in labels if lb.get_text(strip=True)]
        if notes:
            note_str = ", ".join(notes)
            if   i == 0: top_notes  = note_str
            elif i == 1: mid_notes  = note_str
            elif i == 2: base_notes = note_str

    return top_notes, mid_notes, base_notes


def _extract_notes_from_description(soup):
    """
    Fallback: parse notes from the plain-text description paragraph.
    "Top notes are A, B and C; middle notes are D; base notes are E, F."
    """
    desc = soup.find(id="perfume-description-content")
    if not desc:
        return None, None, None
    text = desc.get_text()

    def _section(label):
        m = re.search(
            rf'{label}\s+notes?\s+(?:are|is)\s+(.+?)(?:;|\.|\n)',
            text, re.I,
        )
        if not m:
            return None
        return m.group(1).strip().replace(" and ", ", ")

    return _section("Top"), _section("middle"), _section("base")


# Accords extraction

def _extract_main_accords(soup):
    """
    Extract accord names from the bar-chart section headed "main accords".
    """
    h6 = soup.find("h6", string=lambda t: t and "main accords" in t.lower())
    if not h6:
        return None
    container = h6.find_next_sibling("div")
    if not container:
        return None
    accords = [
        span.get_text(strip=True)
        for span in container.find_all("span", class_="truncate")
        if span.get_text(strip=True)
    ]
    return ", ".join(accords) if accords else None


# Perfumers extraction

def _extract_perfumers(soup):
    """Extract perfumer names from  <a href="/noses/...">  links."""
    names = []
    for a in soup.find_all("a", href=re.compile(r"^/noses/")):
        span = a.find("span")
        name = span.get_text(strip=True) if span else a.get_text(strip=True)
        if name:
            names.append(name)
    return ", ".join(names) if names else None


# Description fields

def _extract_description_fields(soup):
    result = {"year": None, "gender": None, "fragrance_family": None}
    desc = soup.find(id="perfume-description-content")
    if not desc:
        return result
    text = desc.get_text()

    # Year
    m = re.search(r'\b(19[5-9]\d|20[0-4]\d)\b', text)
    if m:
        result["year"] = m.group(1)

    # Fragrance family — text between "is a " and " fragrance"
    m = re.search(r'is\s+a\s+(.+?)\s+fragrance', text, re.I)
    if m:
        result["fragrance_family"] = m.group(1).strip()

    # Gender
    tl = text.lower()
    if "for women and men" in tl or "for men and women" in tl:
        result["gender"] = "unisex"
    elif "for women" in tl:
        result["gender"] = "female"
    elif "for men" in tl:
        result["gender"] = "male"

    return result


# Per-page scrape

def scrape_one(browser, product, scrape_date):
    """
    Scrape one Fragrantica page. Creates a fresh browser context per page
    so Cloudflare cannot correlate requests across the session.

    Parameters
    ----------
    browser     : Playwright browser object (from create_browser_context)
    product     : dict — keys: name, brand, fragrantica_url, [category]
    scrape_date : ISO date string, e.g. "2025-01-15"

    Returns
    -------
    dict or None if the page could not be fetched
    """
    url   = product["fragrantica_url"]
    name  = product["name"]
    brand = product["brand"]

    html = None
    for attempt in range(1, 3):
        context, page = _new_page(browser)
        try:
            html = _fetch_rendered_html(page, url)
            if _page_is_blocked(html):
                logger.warning(f"  Attempt {attempt}: CF block, waiting 20 s…")
                html = None
                time.sleep(20)
                continue
            break
        except Exception as exc:
            logger.warning(f"  Attempt {attempt} failed: {exc}")
            html = None
        finally:
            try:
                page.close()
                context.close()
            except Exception:
                pass
        time.sleep(5)

    if not html or _page_is_blocked(html):
        logger.warning(f"  Failed after retries: {brand} {name}")
        return None

    soup = BeautifulSoup(html, "html.parser")

    # Schema.org aggregate rating
    rating = None
    tag = soup.find(itemprop="ratingValue")
    if tag:
        rating = tag.get_text(strip=True)

    votes = None
    tag = soup.find(itemprop="ratingCount")
    if tag:
        votes = tag.get_text(strip=True)

    # Description: family, year, gender
    desc_fields = _extract_description_fields(soup)

    # Fallback year from full page text
    if not desc_fields["year"]:
        m = re.search(r'(?:launched|created|released)\s+in\s+(\d{4})',
                      soup.get_text(), re.I)
        if m:
            desc_fields["year"] = m.group(1)

    # Fallback gender from full page text
    if not desc_fields["gender"]:
        tl = soup.get_text().lower()
        if "for women and men" in tl or "for men and women" in tl:
            desc_fields["gender"] = "unisex"
        elif "for women" in tl:
            desc_fields["gender"] = "female"
        elif "for men" in tl:
            desc_fields["gender"] = "male"

 
    top, mid, base = _extract_notes_from_pyramid(soup)
    if not top and not mid and not base:
        top, mid, base = _extract_notes_from_description(soup)

    # Accords + perfumers
    main_accords = _extract_main_accords(soup)
    perfumers    = _extract_perfumers(soup)

    # Rating card — love / like / ok / dislike / hate
    sentiment = _extract_card_votes(
        soup, "Rating",
        {"love", "like", "ok", "dislike", "hate"},
    )

    # When-To-Wear card — seasons + day/night
    wear = _extract_card_votes(
        soup, "When To Wear",
        {"winter", "spring", "summer", "fall", "day", "night"},
    )

    return {
        "scrape_date":        scrape_date,
        "name":               name,
        "brand":              brand,
        "category":           product.get("category", ""),
        "fragrantica_url":    url,
        "year_raw":           desc_fields["year"],
        "gender_raw":         desc_fields["gender"],
        "fragrance_family":   desc_fields["fragrance_family"],
        "rating_raw":         rating,
        "votes_raw":          votes,
        "top_notes_raw":      top,
        "middle_notes_raw":   mid,
        "base_notes_raw":     base,
        "main_accords_raw":   main_accords,
        "perfumers":          perfumers,
        "votes_love_raw":     sentiment.get("love"),
        "votes_like_raw":     sentiment.get("like"),
        "votes_ok_raw":       sentiment.get("ok"),
        "votes_dislike_raw":  sentiment.get("dislike"),
        "votes_hate_raw":     sentiment.get("hate"),
        "season_spring_raw":  wear.get("spring"),
        "season_summer_raw":  wear.get("summer"),
        "season_fall_raw":    wear.get("fall"),
        "season_winter_raw":  wear.get("winter"),
        "wear_day_raw":       wear.get("day"),
        "wear_night_raw":     wear.get("night"),
    }


# Batch scraping

def scrape_all(products, scrape_date, limit=None):
    """
    Scrape a list of products with polite rate-limiting.

    Parameters
    ----------
    products    : list of product dicts (from products.json)
    scrape_date : ISO date string
    limit       : int — scrape only first N products (for testing)

    Returns
    -------
    list of raw result dicts
    """
    prods   = products[:limit] if limit else products
    results, skipped = [], []

    pw, browser = create_browser_context()
    try:
        for i, p in enumerate(prods):
            logger.info(f"[{i+1}/{len(prods)}] {p['brand']} — {p['name']}")

            row = scrape_one(browser, p, scrape_date)
            if row:
                results.append(row)
                logger.info(
                    f"  OK: rating={row['rating_raw']} | year={row['year_raw']} | "
                    f"family={row['fragrance_family']} | gender={row['gender_raw']} | "
                    f"accords={row['main_accords_raw']} | "
                    f"love={row['votes_love_raw']} like={row['votes_like_raw']} "
                    f"ok={row['votes_ok_raw']} dislike={row['votes_dislike_raw']} "
                    f"hate={row['votes_hate_raw']} | "
                    f"winter={row['season_winter_raw']} fall={row['season_fall_raw']} "
                    f"day={row['wear_day_raw']} night={row['wear_night_raw']} | "
                    f"top_notes={(row['top_notes_raw'] or '')[:50]}"
                )
            else:
                skipped.append(f"{p['brand']} — {p['name']}")
                logger.warning("  SKIPPED")

            if i < len(prods) - 1:
                time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))

            if (i + 1) % PAUSE_EVERY == 0 and i < len(prods) - 1:
                pause = random.uniform(PAUSE_MIN, PAUSE_MAX)
                logger.info(
                    f"  --- Pausing {pause:.0f} s "
                    f"({len(results)} ok, {len(skipped)} skipped) ---"
                )
                time.sleep(pause)

    finally:
        browser.close()
        pw.stop()

    logger.info(
        f"Fragrantica done: {len(results)}/{len(prods)} ok, "
        f"{len(skipped)} skipped"
    )
    for s in skipped:
        logger.info(f"  SKIPPED: {s}")

    return results


# Persistence

def save_raw(data, path="raw_data/fragrantica_raw.csv"):
    """Append scraped rows to CSV; creates file with header on first call."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df     = pd.DataFrame(data)
    exists = os.path.exists(path)
    df.to_csv(path, mode="a", header=not exists, index=False, encoding="utf-8")
    logger.info(f"Saved {len(df)} rows → {path}")
    return df



if __name__ == "__main__":
    with open("products.json") as f:
        products = json.load(f)

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    data  = scrape_all(products, today, limit=3)

    if data:
        df = save_raw(data)
        cols = [
            "name", "brand", "year_raw", "fragrance_family", "gender_raw",
            "rating_raw", "votes_raw", "main_accords_raw", "perfumers",
            "votes_love_raw", "votes_like_raw", "votes_ok_raw",
            "votes_dislike_raw", "votes_hate_raw",
            "season_winter_raw", "season_fall_raw",
            "wear_day_raw", "wear_night_raw",
        ]
        print(df[[c for c in cols if c in df.columns]].to_string())
