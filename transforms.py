"""
Data cleaning and transformation functions
Each function transforms one raw field into a clean, typed value.
"""

import re
import pandas as pd
import numpy as np

from fx_rates import get_rates_for_date

PRICE_BANDS = [
    ("budget",        0.00,   0.30),
    ("affordable",    0.30,   0.60),
    ("mid_range",     0.60,   1.00),
    ("premium",       1.00,   2.00),
    ("luxury",        2.00,   5.00),
    ("ultra_luxury",  5.00, 999.0),
]

def transform_rating(raw):
    """Convert a rating string to a float in [0, 5]; None if invalid."""
    try:
        v = float(str(raw).strip())
        return v if 0 <= v <= 5 else None
    except (ValueError, TypeError):
        return None


def transform_votes(raw):
    """
    Convert a vote review-count string to int.
    """
    if pd.isna(raw) or raw is None:
        return None
    try:
        return int(str(raw).replace(",", "").replace(".", "").strip())
    except (ValueError, TypeError):
        return None


def transform_vote_count(raw):
    """
    Convert a Fragrantica display count to int."""
    if pd.isna(raw) or raw is None:
        return None
    t = str(raw).strip().lower().replace(",", "")
    if "k" in t:
        try:
            return int(float(t.replace("k", "")) * 1000)
        except (ValueError, TypeError):
            return None
    try:
        return int(float(t))
    except (ValueError, TypeError):
        return None


def transform_year(raw):
    """Convert a year string to int; None if outside [1900, 2030]."""
    try:
        y = int(str(raw).strip())
        return y if 1900 <= y <= 2030 else None
    except (ValueError, TypeError):
        return None


def transform_price(raw):
    """Extract a numeric price from strings like 'GBP 76.50', '£89.99'."""
    if pd.isna(raw) or raw is None:
        return None
    text = str(raw)
    for s in ["GBP", "AED", "USD", "EUR", "\u00a3", ","]:
        text = text.replace(s, "")
    m = re.search(r"(\d+\.?\d*)", text.strip())
    return float(m.group(1)) if m else None


def transform_bottle_size(raw):
    """
    Extract bottle size in ml from a product title string.

    Converts fl oz to ml.  Defaults to 100 ml when unparseable.
    """
    if pd.isna(raw):
        return 100.0
    text = str(raw).lower()
    m = re.search(r"(\d+\.?\d*)\s*ml", text)
    if m:
        s = float(m.group(1))
        return s if 5 <= s <= 500 else 100.0
    m = re.search(r"(\d+\.?\d*)\s*(?:fl\.?\s*oz)", text)
    if m:
        return round(float(m.group(1)) * 29.5735, 1)
    return 100.0


def convert_to_eur(price, currency, scrape_date=None):
    if pd.isna(price) or price is None:
        return None
    rates = get_rates_for_date(scrape_date)
    rate = rates.get(str(currency).upper().strip())
    return round(float(price) * rate, 2) if rate else None


def compute_price_per_ml(price_eur, bottle_ml):
    """Calculate EUR price per millilitre."""
    if pd.isna(price_eur) or pd.isna(bottle_ml) or bottle_ml <= 0:
        return None
    return round(price_eur / bottle_ml, 4)


def classify_price_band(ppm):
    """Classify price-per-ml into a named market segment."""
    if pd.isna(ppm):
        return "unknown"
    for label, lo, hi in PRICE_BANDS:
        if lo <= ppm < hi:
            return label
    return "unknown"


def transform_amazon_rating(raw):
    """Extract numeric rating from '4.7 out of 5 stars'."""
    if pd.isna(raw):
        return None
    m = re.search(r"(\d+\.?\d*)\s*out\s*of\s*5", str(raw))
    return float(m.group(1)) if m else None


def clean_text_field(raw):
    """Strip whitespace; return None for empty / NaN values."""
    if pd.isna(raw) or raw is None:
        return None
    s = str(raw).strip()
    return s if s else None


def _drop_empty_fragrantica_rows(c):
    """Drop Fragrantica rows with no rating AND no votes.
 
    Same logic as _drop_empty_amazon_rows — a fragrance with neither a
    rating nor a vote count can't feed any sentiment or rating analysis.
    """
    import logging
    has_rating = c["rating"].notna() if "rating" in c.columns else False
    has_votes  = c["votes"].notna()  if "votes"  in c.columns else False
    before = len(c)
    c = c[has_rating | has_votes].copy()
    dropped = before - len(c)
    if dropped:
        logging.getLogger("transforms").info(
            f"clean_fragrantica: dropped {dropped} noise rows (no rating and no votes)"
        )
    return c

def clean_fragrantica(df):
    """Apply all transformations to a raw Fragrantica DataFrame."""
    c = df.copy()

    c["rating"] = c["rating_raw"].apply(transform_rating)
    c["votes"]  = c["votes_raw"].apply(transform_votes)
    c["year"]   = c["year_raw"].apply(transform_year)
    c["gender"] = c["gender_raw"].fillna("unknown").str.strip().str.lower()

    for col in ["top_notes_raw", "middle_notes_raw", "base_notes_raw"]:
        c[col.replace("_raw", "")] = c[col].apply(clean_text_field).fillna("")

    c["main_accords"]    = c["main_accords_raw"].apply(clean_text_field).fillna("")
    c["fragrance_family"] = c["fragrance_family"].apply(clean_text_field).fillna("")
    c["perfumers"]       = c["perfumers"].apply(clean_text_field).fillna("")

    for emotion in ["love", "like", "ok", "dislike", "hate"]:
        raw_col = f"votes_{emotion}_raw"
        out_col = f"votes_{emotion}"
        if raw_col in c.columns:
            c[out_col] = c[raw_col].apply(transform_vote_count)
        else:
            c[out_col] = None

    for season in ["spring", "summer", "fall", "winter"]:
        raw_col = f"season_{season}_raw"
        out_col = f"season_{season}"
        if raw_col in c.columns:
            c[out_col] = c[raw_col].apply(transform_vote_count)
        else:
            c[out_col] = None

    for period in ["day", "night"]:
        raw_col = f"wear_{period}_raw"
        out_col = f"wear_{period}"
        if raw_col in c.columns:
            c[out_col] = c[raw_col].apply(transform_vote_count)
        else:
            c[out_col] = None

    keep = [
        "scrape_date", "name", "brand", "category", "fragrantica_url",
        "year", "rating", "votes", "gender",
        "fragrance_family", "main_accords", "perfumers",
        "top_notes", "middle_notes", "base_notes",
        "votes_love", "votes_like", "votes_ok", "votes_dislike", "votes_hate",
        "season_spring", "season_summer", "season_fall", "season_winter",
        "wear_day", "wear_night",
    ]
    c = _drop_empty_fragrantica_rows(c)
    return c[[col for col in keep if col in c.columns]]


_COUNTRY_CURRENCY = {"USA": "USD", "UAE": "AED"}

def _drop_empty_amazon_rows(c):
    """
    Drop Amazon rows with no price AND no rating.
    Both columns empty means the scraper found a card but failed to
    extract anything useful from it.
    """
    has_price  = c["price"].notna()  if "price"  in c.columns else False
    has_rating = c["amazon_rating"].notna() if "amazon_rating" in c.columns else False
    before = len(c)
    c = c[has_price | has_rating].copy()
    dropped = before - len(c)
    if dropped:
        import logging
        logging.getLogger("transforms").info(
            f"clean_amazon: dropped {dropped} empty rows "
            f"(no price and no rating)"
        )
    return c

def clean_amazon(df):
    """Apply all transformations to a raw Amazon DataFrame."""
    c = df.copy()

    required_cols = [
        "scrape_date", "name", "brand", "category", "country",
        "currency", "price_raw", "bottle_size_raw",
        "amazon_rating_raw", "amazon_reviews_raw",
        "availability", "amazon_url",
    ]
    for col in required_cols:
        if col not in c.columns:
            c[col] = None

    def _fill_currency(row):
        cur = row.get("currency")
        if cur and str(cur).strip() and str(cur).lower() != "nan":
            return cur
        return _COUNTRY_CURRENCY.get(row.get("country"), None)
    c["currency"] = c.apply(_fill_currency, axis=1)

    c["price"]          = c["price_raw"].apply(transform_price)
    c["bottle_size_ml"] = c["bottle_size_raw"].apply(transform_bottle_size)
    c["amazon_rating"]  = c["amazon_rating_raw"].apply(transform_amazon_rating)
    c["amazon_reviews"] = c["amazon_reviews_raw"].apply(transform_votes)

    c["price_eur"] = c.apply(
        lambda r: convert_to_eur(r["price"], r["currency"], r["scrape_date"]),
        axis=1,
    )
    c["price_per_ml"] = c.apply(
        lambda r: compute_price_per_ml(r["price_eur"], r["bottle_size_ml"]),
        axis=1,
    )

    keep = [
        "scrape_date", "name", "brand", "category", "country", "currency",
        "price", "price_eur", "bottle_size_ml", "price_per_ml",
        "amazon_rating", "amazon_reviews", "availability", "amazon_url",
    ]
    c = _drop_empty_amazon_rows(c)
    return c[[col for col in keep if col in c.columns]]


def merge_datasets(frag_df, amz_df):
    """Left-join Amazon prices onto Fragrantica data on name + brand + date."""
    merged = pd.merge(
        amz_df, frag_df,
        on=["name", "brand", "scrape_date"],
        how="left",
        suffixes=("", "_frag"),
    )
    if "category_frag" in merged.columns:
        merged["category"] = merged["category"].fillna(merged["category_frag"])
        merged.drop(columns=["category_frag"], inplace=True, errors="ignore")
    return merged
