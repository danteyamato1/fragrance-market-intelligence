"""
Live exchange rate fetching with caching

Replaces the hardcoded RATES_TO_EUR dict in transforms.py with live
rates pulled from the European Central Bank's public reference feed.
Rates are cached per-day so the pipeline doesn't hammer the API and
every row in the database can be tagged with the exact rate that was
used on the day of the scrape.
"""

from __future__ import annotations
import os
import json
import logging
import datetime as dt
from typing import Dict

logger = logging.getLogger("fx_rates")

CACHE_PATH = "fx_cache.json"
ECB_URL = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml"

FALLBACK_RATES = {
    "EUR": 1.00,
    "USD": 0.853,
    "GBP": 1.160,
    "AED": 0.232,
}


def _fetch_ecb_rates() -> Dict[str, float]:
    """Fetch today's rates from the ECB reference XML feed.

    ECB publishes EUR-to-X rates (how many X you get for 1 EUR). We
    invert them so we can express X-to-EUR (what 1 X is worth in EUR),
    which is the form our pipeline uses.
    """
    import urllib.request
    import xml.etree.ElementTree as ET

    req = urllib.request.Request(
        ECB_URL,
        headers={"User-Agent": "fragrance-market-intelligence/1.0"},
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        xml_bytes = resp.read()

    root = ET.fromstring(xml_bytes)
    ns = {"ecb": "http://www.ecb.int/vocabulary/2002-08-01/eurofxref"}
    cube_day = root.find(".//ecb:Cube[@time]", ns)
    if cube_day is None:
        raise RuntimeError("ECB feed missing Cube[@time] element")

    eur_to_x = {"EUR": 1.0}
    for cube in cube_day.findall("ecb:Cube", ns):
        ccy = cube.get("currency")
        rate = float(cube.get("rate"))
        if ccy:
            eur_to_x[ccy] = rate

    if "USD" in eur_to_x:
        usd_per_eur = eur_to_x["USD"]
        eur_to_x["AED"] = 3.6725 * usd_per_eur 

    x_to_eur = {ccy: round(1.0 / rate, 6) for ccy, rate in eur_to_x.items()}
    x_to_eur["EUR"] = 1.0

    keep = {"EUR", "USD", "GBP", "AED"}
    return {k: v for k, v in x_to_eur.items() if k in keep}


def _load_cache() -> dict:
    if not os.path.exists(CACHE_PATH):
        return {}
    try:
        with open(CACHE_PATH, "r") as f:
            return json.load(f)
    except Exception as e:
        logger.warning("fx_cache.json unreadable: %s", e)
        return {}


def _save_cache(cache: dict) -> None:
    try:
        with open(CACHE_PATH, "w") as f:
            json.dump(cache, f, indent=2, sort_keys=True)
    except Exception as e:
        logger.warning("Could not write fx_cache.json: %s", e)


def get_rates_for_date(date: str | None = None) -> Dict[str, float]:
    """Return X-to-EUR rates for the given date (default: today)."""
    date = date or dt.date.today().isoformat()
    cache = _load_cache()

    if date in cache:
        return cache[date]

    today = dt.date.today().isoformat()
    if date == today:
        try:
            rates = _fetch_ecb_rates()
            cache[date] = rates
            _save_cache(cache)
            logger.info("Fetched live FX rates from ECB: %s", rates)
            return rates
        except Exception as e:
            logger.warning("ECB fetch failed: %s — falling back", e)

    earlier_dates = sorted([d for d in cache.keys() if d < date], reverse=True)
    if earlier_dates:
        logger.info("Using cached rates from %s for target %s",
                    earlier_dates[0], date)
        return cache[earlier_dates[0]]

    logger.warning("No live or cached rates available — using fallback")
    return FALLBACK_RATES.copy()


if __name__ == "__main__":
    # CLI sanity check — `python fx_rates.py`
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    rates = get_rates_for_date()
    print("Today's X → EUR rates:")
    for ccy, rate in sorted(rates.items()):
        print(f"  1 {ccy} = {rate:.4f} EUR")
