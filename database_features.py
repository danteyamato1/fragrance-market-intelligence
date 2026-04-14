"""
database_features.py - Adds the fragrance_features table
Import this from database.py to persist the output
of feature_engineering.engineer_features() alongside the raw cleaned data.
"""

import sqlite3
import logging
import pandas as pd

logger = logging.getLogger("database_features")


_BASE_SCHEMA = """
CREATE TABLE IF NOT EXISTS fragrance_features (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    fragrance_id   INTEGER,
    scrape_date    TEXT NOT NULL,
    brand          TEXT,
    name           TEXT,
    gender_id      INTEGER,
    origin_id      INTEGER,
    value_score              REAL,
    sentiment_polarisation   REAL,
    crowd_pleaser_index      REAL,
    usa_uae_gap_pct          REAL,
    season_dominant          TEXT,
    note_count               INTEGER,
    years_on_market          INTEGER,
    rating_scaled            REAL,
    price_per_ml_eur_scaled  REAL,
    votes_scaled             REAL,
    FOREIGN KEY (fragrance_id) REFERENCES fragrances(fragrance_id),
    UNIQUE (fragrance_id, scrape_date)
);
"""


def ensure_features_table(conn: sqlite3.Connection,
                          features_df: pd.DataFrame) -> None:
    """
    Create the features table and ALTER in any one-hot columns that
    don't yet exist.
    """
    cur = conn.cursor()
    cur.executescript(_BASE_SCHEMA)

    cur.execute("PRAGMA table_info(fragrance_features)")
    existing = {row[1] for row in cur.fetchall()}

    dynamic_cols = [c for c in features_df.columns
                    if c.startswith(("fam_", "band_"))
                    and c not in existing]
    for col in dynamic_cols:
        try:
            cur.execute(
                f'ALTER TABLE fragrance_features ADD COLUMN "{col}" INTEGER'
            )
            logger.info("Added feature column %s", col)
        except sqlite3.OperationalError as e:
            logger.debug("Skipped ALTER for %s: %s", col, e)

    conn.commit()


def load_features(conn: sqlite3.Connection,
                  features_df: pd.DataFrame,
                  scrape_date: str) -> int:
    """
    Insert today's feature rows. Returns number of rows written.

    Uses INSERT OR REPLACE on (fragrance_id, scrape_date) so re-runs on
    the same day overwrite rather than duplicate.
    """
    if features_df is None or features_df.empty:
        logger.warning("load_features: empty DataFrame, nothing to write")
        return 0

    ensure_features_table(conn, features_df)

    cur = conn.cursor()
    cur.execute("SELECT fragrance_id, name, brand_id FROM fragrances")
    frag_rows = cur.fetchall()
    cur.execute("SELECT brand_id, brand_name FROM brands")
    brand_map = {b[0]: b[1].lower() for b in cur.fetchall()}
    key_to_id = {
        (brand_map.get(bid, "").lower(), name.lower().strip()): fid
        for fid, name, bid in frag_rows
    }

    cur.execute("PRAGMA table_info(fragrance_features)")
    tbl_cols = [r[1] for r in cur.fetchall()
                if r[1] not in ("id",)]

    written = 0
    for _, row in features_df.iterrows():
        brand = str(row.get("brand", "")).lower().strip()
        name  = str(row.get("name",  "")).lower().strip()
        fid   = key_to_id.get((brand, name))

        record = {"fragrance_id": fid, "scrape_date": scrape_date}
        for col in tbl_cols:
            if col in ("fragrance_id", "scrape_date"):
                continue
            if col in row.index:
                val = row[col]
                if pd.isna(val):
                    val = None
                record[col] = val

        cols = ", ".join(f'"{c}"' for c in record)
        placeholders = ", ".join("?" for _ in record)
        try:
            cur.execute(
                f"INSERT OR REPLACE INTO fragrance_features ({cols}) "
                f"VALUES ({placeholders})",
                list(record.values()),
            )
            written += 1
        except sqlite3.Error as e:
            logger.warning("Insert failed for %s / %s: %s", brand, name, e)

    conn.commit()
    logger.info("load_features: wrote %d rows for %s", written, scrape_date)
    return written
