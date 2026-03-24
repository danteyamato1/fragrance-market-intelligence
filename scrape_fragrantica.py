import re
import time
import random
import json
import logging
import os
import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("fragrantica")

MIN_DELAY = 5.0
MAX_DELAY = 12.0
RETRY_DELAYS = [15, 30, 60]
MAX_RETRIES = 3

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.5; rv:126.0) Gecko/20100101 Firefox/126.0",
]


def create_session():
    session = requests.Session()
    ua = random.choice(USER_AGENTS)
    session.headers.update({
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "DNT": "1",
    })
    return session


def warm_up_session(session):
    logger.info("Visiting Fragrantica homepage to get cookies...")
    try:
        resp = session.get("https://www.fragrantica.com/", timeout=15)
        resp.raise_for_status()
        logger.info(f"  Session ready. {len(session.cookies)} cookies set.")
        time.sleep(random.uniform(3, 6))
    except Exception as e:
        logger.warning(f"  Homepage failed: {e}")


def fetch_with_retry(session, url, label):
    for attempt in range(MAX_RETRIES + 1):
        try:
            session.headers["Referer"] = "https://www.fragrantica.com/"
            resp = session.get(url, timeout=20)

            if resp.status_code == 200:
                return resp.text

            if resp.status_code == 403:
                if attempt < MAX_RETRIES:
                    wait = RETRY_DELAYS[attempt]
                    logger.warning(f"  403 on attempt {attempt+1}. Waiting {wait}s...")
                    time.sleep(wait)
                    continue
                else:
                    logger.error(f"  403 after {MAX_RETRIES} retries. Skipping {label}.")
                    return None

            resp.raise_for_status()

        except requests.exceptions.Timeout:
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAYS[min(attempt, len(RETRY_DELAYS)-1)])
                continue
            return None
        except Exception as e:
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAYS[min(attempt, len(RETRY_DELAYS)-1)])
                continue
            return None

    return None


def extract_notes(soup, label):
    for b in soup.find_all("b"):
        if label.lower() in b.get_text(strip=True).lower():
            parent = b.find_parent("div")
            if parent:
                spans = parent.find_all("span")
                notes = [
                    s.get_text(strip=True) for s in spans
                    if s.get_text(strip=True)
                    and len(s.get_text(strip=True)) > 1
                    and s.get_text(strip=True).lower() != label.lower()
                ]
                return ", ".join(notes)
    return None


def scrape_one(session, product, scrape_date):
    url = product["fragrantica_url"]
    name = product["name"]
    brand = product["brand"]

    page_html = fetch_with_retry(session, url, f"{brand} {name}")
    if page_html is None:
        return None

    soup = BeautifulSoup(page_html, "html.parser")
    page_text = soup.get_text()

    rating = None
    tag = soup.find(itemprop="ratingValue")
    if tag:
        try:
            rating = tag.get_text(strip=True)
        except:
            pass

    votes = None
    tag = soup.find(itemprop="ratingCount")
    if tag:
        try:
            votes = tag.get_text(strip=True)
        except:
            pass

    year = None
    m = re.search(r'(?:launched|created|released)\s+in\s+(\d{4})', page_text, re.IGNORECASE)
    if m:
        year = m.group(1)

    gender = None
    tl = page_text.lower()
    if "for women and men" in tl or "for men and women" in tl:
        gender = "unisex"
    elif "for women" in tl:
        gender = "female"
    elif "for men" in tl:
        gender = "male"

    seasons = {}
    for s in ["spring", "summer", "fall", "winter"]:
        sm = re.search(rf'{s}\s*[:\s]*(\d+)', page_text, re.IGNORECASE)
        seasons[s] = sm.group(1) if sm else None

    return {
        "scrape_date": scrape_date,
        "name": name,
        "brand": brand,
        "category": product.get("category", ""),
        "fragrantica_url": url,
        "year": year,
        "rating": rating,
        "votes": votes,
        "gender": gender,
        "top_notes": extract_notes(soup, "Top"),
        "middle_notes": extract_notes(soup, "Middle"),
        "base_notes": extract_notes(soup, "Base"),
        "season_spring": seasons["spring"],
        "season_summer": seasons["summer"],
        "season_fall": seasons["fall"],
        "season_winter": seasons["winter"],
    }


def scrape_all(products, scrape_date):
    session = create_session()
    warm_up_session(session)

    results = []
    total = len(products)
    skipped = []

    for i, product in enumerate(products):
        logger.info(f"[{i+1}/{total}] {product['brand']} - {product['name']}")

        row = scrape_one(session, product, scrape_date)

        if row:
            results.append(row)
            logger.info(f"  OK: rating={row['rating']}, gender={row['gender']}, "
                        f"top_notes={'yes' if row['top_notes'] else 'none'}")
        else:
            skipped.append(f"{product['brand']} - {product['name']}")
            logger.warning(f"  SKIPPED")

        if i < total - 1:
            time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))

        if (i + 1) % 20 == 0 and i < total - 1:
            pause = random.uniform(20, 40)
            logger.info(f"  --- Pause {pause:.0f}s ({len(results)} ok, {len(skipped)} skipped) ---")
            time.sleep(pause)

    logger.info(f"Done: {len(results)}/{total} succeeded")
    if skipped:
        logger.info(f"Skipped {len(skipped)}:")
        for s in skipped:
            logger.info(f"  - {s}")

    return results


def save_raw(results, filepath):
    df = pd.DataFrame(results)
    file_exists = os.path.exists(filepath)
    df.to_csv(filepath, mode="a", header=not file_exists, index=False, encoding="utf-8")
    logger.info(f"Saved {len(df)} rows to {filepath}")


if __name__ == "__main__":
    os.makedirs("raw_data", exist_ok=True)

    with open("products.json", "r") as f:
        products = json.load(f)

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    data = scrape_all(products, today)

    if data:
        save_raw(data, "raw_data/fragrantica_raw.csv")
