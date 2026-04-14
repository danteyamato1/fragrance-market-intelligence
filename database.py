"""
database.py - SQLite database loading with normalised schema
==============================================================
Tables
------
    brands           - one row per brand
    fragrances       - one row per fragrance (name + brand)
    fragrantica_data - daily Fragrantica scrape (rating, notes, votes, …)
    amazon_prices    - daily Amazon price data per country

New columns (fragrantica_data)
------------------------------
    fragrance_family TEXT    - e.g. "Aromatic Fougere"
    main_accords     TEXT    - comma-separated accord names
    perfumers        TEXT    - comma-separated creator names
    votes_love       INTEGER - community sentiment breakdown
    votes_like       INTEGER
    votes_ok         INTEGER
    votes_dislike    INTEGER
    votes_hate       INTEGER
    wear_day         INTEGER - When To Wear: day votes
    wear_night       INTEGER - When To Wear: night votes
"""

import sqlite3
import logging

logger  = logging.getLogger("database")
DB_PATH = "fragrance_market.db"

_NEW_FRAGRANTICA_COLS = [
    ("fragrance_family", "TEXT"),
    ("main_accords",     "TEXT"),
    ("perfumers",        "TEXT"),
    ("votes_love",       "INTEGER"),
    ("votes_like",       "INTEGER"),
    ("votes_ok",         "INTEGER"),
    ("votes_dislike",    "INTEGER"),
    ("votes_hate",       "INTEGER"),
    ("wear_day",         "INTEGER"),
    ("wear_night",       "INTEGER"),
]


def create_database(path=DB_PATH):
    """
    Create (or open) the SQLite database and ensure all tables + columns exist.

    Safe to call repeatedly:
    - Tables use  CREATE TABLE IF NOT EXISTS
    - New columns use  ALTER TABLE … ADD COLUMN  inside try/except so
      running this on an existing database simply skips columns that
      are already present (SQLite raises OperationalError for duplicates)
    """
    conn = sqlite3.connect(path)
    c    = conn.cursor()

    c.execute("""
        CREATE TABLE IF NOT EXISTS brands (
            brand_id   INTEGER PRIMARY KEY AUTOINCREMENT,
            brand_name TEXT    UNIQUE NOT NULL,
            category   TEXT    NOT NULL
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS fragrances (
            fragrance_id    INTEGER PRIMARY KEY AUTOINCREMENT,
            name            TEXT    NOT NULL,
            brand_id        INTEGER NOT NULL,
            gender          TEXT,
            year            INTEGER,
            bottle_ml       REAL,
            fragrantica_url TEXT,
            FOREIGN KEY (brand_id) REFERENCES brands(brand_id)
        )
    """)


    c.execute("""
        CREATE TABLE IF NOT EXISTS fragrantica_data (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            fragrance_id     INTEGER NOT NULL,
            scrape_date      TEXT    NOT NULL,

            -- Community aggregate rating
            rating           REAL,
            votes            INTEGER,

            -- Fragrance composition
            fragrance_family TEXT,
            main_accords     TEXT,
            perfumers        TEXT,
            top_notes        TEXT,
            middle_notes     TEXT,
            base_notes       TEXT,

            -- Sentiment breakdown
            votes_love       INTEGER,
            votes_like       INTEGER,
            votes_ok         INTEGER,
            votes_dislike    INTEGER,
            votes_hate       INTEGER,

            -- Seasonality (When To Wear widget vote counts)
            season_spring    INTEGER,
            season_summer    INTEGER,
            season_fall      INTEGER,
            season_winter    INTEGER,

            -- Time of day
            wear_day         INTEGER,
            wear_night       INTEGER,

            FOREIGN KEY (fragrance_id) REFERENCES fragrances(fragrance_id)
        )
    """)


    for col_name, col_type in _NEW_FRAGRANTICA_COLS:
        try:
            c.execute(
                f"ALTER TABLE fragrantica_data ADD COLUMN {col_name} {col_type}"
            )
            logger.debug(f"  Migrated: added column {col_name} to fragrantica_data")
        except sqlite3.OperationalError as exc:
            if "duplicate column" in str(exc).lower():
                pass
            else:
                raise

    c.execute("""
        CREATE TABLE IF NOT EXISTS amazon_prices (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            fragrance_id   INTEGER NOT NULL,
            scrape_date    TEXT    NOT NULL,
            country        TEXT    NOT NULL,
            currency       TEXT    NOT NULL,
            price          REAL,
            price_eur      REAL,
            price_per_ml   REAL,
            amazon_rating  REAL,
            amazon_reviews INTEGER,
            availability   TEXT,
            FOREIGN KEY (fragrance_id) REFERENCES fragrances(fragrance_id)
        )
    """)

    conn.commit()
    return conn



def _get_fragrance_id(cursor, name, brand):
    """Return fragrance_id for (name, brand) or None if not found."""
    cursor.execute("""
        SELECT f.fragrance_id
        FROM   fragrances f
        JOIN   brands     b ON f.brand_id = b.brand_id
        WHERE  f.name = ? AND b.brand_name = ?
    """, (name, brand))
    row = cursor.fetchone()
    return row[0] if row else None


def _safe_int(val):
    """Convert to int, return None on failure."""
    try:
        return int(val) if val is not None else None
    except (ValueError, TypeError):
        return None


def _safe_float(val):
    """Convert to float, return None on failure."""
    try:
        return float(val) if val is not None else None
    except (ValueError, TypeError):
        return None


def _str_or_none(val):
    """Return stripped string, or None for empty / NaN."""
    if val is None:
        return None
    import math
    try:
        if math.isnan(float(val)):
            return None
    except (ValueError, TypeError):
        pass
    s = str(val).strip()
    return s if s else None



def load_to_database(frag_df, amz_df=None, path=DB_PATH):
    """
    Load cleaned DataFrames into SQLite.

    Parameters
    ----------
    frag_df : pandas DataFrame - output of transforms.clean_fragrantica()
    amz_df  : pandas DataFrame - output of transforms.clean_amazon()
    path    : str - path to .db file

    Returns
    -------
    dict  {table_name: row_count}
    """
    conn = create_database(path) 
    c    = conn.cursor()


    for _, r in frag_df[["brand", "category"]].drop_duplicates().iterrows():
        c.execute(
            "INSERT OR IGNORE INTO brands (brand_name, category) VALUES (?, ?)",
            (r["brand"], r["category"]),
        )


    for _, r in frag_df.drop_duplicates(subset=["name", "brand"]).iterrows():
        c.execute("SELECT brand_id FROM brands WHERE brand_name = ?", (r["brand"],))
        row = c.fetchone()
        if not row:
            continue
        c.execute(
            """INSERT OR IGNORE INTO fragrances
               (name, brand_id, gender, year, fragrantica_url)
               VALUES (?, ?, ?, ?, ?)""",
            (
                r["name"], row[0],
                _str_or_none(r.get("gender")),
                _safe_int(r.get("year")),
                _str_or_none(r.get("fragrantica_url")),
            ),
        )

    for _, r in frag_df.iterrows():
        fid = _get_fragrance_id(c, r["name"], r["brand"])
        if not fid:
            logger.warning(f"  No fragrance_id for {r['brand']} — {r['name']}")
            continue

        c.execute("""
            INSERT INTO fragrantica_data (
                fragrance_id,  scrape_date,
                rating,        votes,
                fragrance_family, main_accords, perfumers,
                top_notes,     middle_notes,  base_notes,
                votes_love,    votes_like,    votes_ok,
                votes_dislike, votes_hate,
                season_spring, season_summer,
                season_fall,   season_winter,
                wear_day,      wear_night
            ) VALUES (
                ?, ?,
                ?, ?,
                ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?,
                ?, ?,
                ?, ?,
                ?, ?,
                ?, ?
            )""",
            (
                fid,
                _str_or_none(r.get("scrape_date")),
                _safe_float(r.get("rating")),
                _safe_int(r.get("votes")),
                _str_or_none(r.get("fragrance_family")),
                _str_or_none(r.get("main_accords")),
                _str_or_none(r.get("perfumers")),
                _str_or_none(r.get("top_notes")),
                _str_or_none(r.get("middle_notes")),
                _str_or_none(r.get("base_notes")),
                _safe_int(r.get("votes_love")),
                _safe_int(r.get("votes_like")),
                _safe_int(r.get("votes_ok")),
                _safe_int(r.get("votes_dislike")),
                _safe_int(r.get("votes_hate")),
                _safe_int(r.get("season_spring")),
                _safe_int(r.get("season_summer")),
                _safe_int(r.get("season_fall")),
                _safe_int(r.get("season_winter")),
                _safe_int(r.get("wear_day")),
                _safe_int(r.get("wear_night")),
            ),
        )

    if amz_df is not None and not amz_df.empty:
        for _, r in amz_df.iterrows():
            fid = _get_fragrance_id(c, r["name"], r["brand"])
            if not fid:
                continue
            c.execute("""
                INSERT INTO amazon_prices (
                    fragrance_id,  scrape_date,
                    country,       currency,
                    price,         price_eur,     price_per_ml,
                    amazon_rating, amazon_reviews, availability
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    fid,
                    _str_or_none(r.get("scrape_date")),
                    _str_or_none(r.get("country")),
                    _str_or_none(r.get("currency")),
                    _safe_float(r.get("price")),
                    _safe_float(r.get("price_eur")),
                    _safe_float(r.get("price_per_ml")),
                    _safe_float(r.get("amazon_rating")),
                    _safe_int(r.get("amazon_reviews")),
                    _str_or_none(r.get("availability")),
                ),
            )

    conn.commit()

    summary = {}
    for table in ["brands", "fragrances", "fragrantica_data", "amazon_prices"]:
        c.execute(f"SELECT COUNT(*) FROM {table}")
        summary[table] = c.fetchone()[0]
        logger.info(f"  {table}: {summary[table]} rows")

    conn.close()
    return summary
