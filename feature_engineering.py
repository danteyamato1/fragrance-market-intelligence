"""
Runs AFTER transforms.py and BEFORE database.

Responsibilities
----------------
1. Categorical encoding
     - LabelEncoder  : gender, brand_origin
     - OneHotEncoder : fragrance_family, price_band
2. Numerical scaling
     - MinMaxScaler  : rating, price_per_ml_eur, votes
3. Derived features
     - value_score, sentiment_polarisation, crowd_pleaser_index,
       usa_uae_gap_pct, season_dominant, note_count, years_on_market
4. Feature selection
     - Drops near-constant columns (variance threshold)
     - Drops one of each highly-correlated pair (|r| > 0.95)

Every encoder/scaler is fit once and persisted in the returned dict so the
same transformation can be re-applied deterministically to future daily
snapshots.
"""

from __future__ import annotations

import logging
import datetime as dt
import numpy as np
import pandas as pd
from sklearn.preprocessing import LabelEncoder, OneHotEncoder, MinMaxScaler
from sklearn.feature_selection import VarianceThreshold

logger = logging.getLogger("feature_engineering")

ARABIC_BRANDS = {
    "lattafa", "armaf", "al haramain", "rasasi", "ajmal",
    "swiss arabian", "afnan", "khadlaj", "nabeel", "arabian oud",
    "french avenue", "aromatix x french avenue", "ibraheem alqurashi",
}
 
NICHE_BRANDS = {

    "creed", "parfums de marly", "xerjoff", "amouage",
    "maison francis kurkdjian", "initio", "roja parfums", "clive christian",
    "nasomatto", "nishane", "by kilian", "frederic malle",
    "mind games", "bdk parfums", "memo paris", "diptyque",
    "tom ford",
}

def _classify_origin(brand: str) -> str:
    """Three-way brand origin classification.
 
    Data-driven rather than per-product: ignores whatever's in products.json
    and uses a curated brand set. This is the correct approach because
    'brand origin' is a property of the brand, not of individual products —
    every Creed product should be classified identically, regardless of
    what category field was typed into products.json.
    """
    if not isinstance(brand, str):
        return "unknown"
    b = brand.strip().lower()
    if b in ARABIC_BRANDS:
        return "arabic"
    if b in NICHE_BRANDS:
        return "niche"
    return "designer"

def _sentiment_polarisation(row):
    total = sum([row.get(c, 0) or 0 for c in
                 ("votes_love", "votes_like", "votes_ok",
                  "votes_dislike", "votes_hate")])
    if total == 0:
        return np.nan
    return ((row.get("votes_love", 0) or 0) -
            (row.get("votes_hate", 0) or 0)) / total


def _crowd_pleaser(row):
    total = sum([row.get(c, 0) or 0 for c in
                 ("votes_love", "votes_like", "votes_ok",
                  "votes_dislike", "votes_hate")])
    if total == 0:
        return np.nan
    return ((row.get("votes_love", 0) or 0) +
            (row.get("votes_like", 0) or 0)) / total


def _season_dominant(row):
    seasons = {
        "spring": row.get("season_spring", 0) or 0,
        "summer": row.get("season_summer", 0) or 0,
        "fall":   row.get("season_fall", 0) or 0,
        "winter": row.get("season_winter", 0) or 0,
    }
    if sum(seasons.values()) == 0:
        return "unknown"
    return max(seasons, key=seasons.get)


def _note_count(row):
    notes = set()
    for col in ("top_notes", "middle_notes", "base_notes"):
        v = row.get(col)
        if isinstance(v, str) and v.strip():
            notes.update(n.strip().lower() for n in v.split(",") if n.strip())
    return len(notes)


def engineer_features(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """
    Take a cleaned, merged DataFrame and return (features_df, encoders).

    `encoders` is a dict containing every fitted transformer, ready to be
    pickled for reuse on tomorrow's scrape.
    """
    if df is None or df.empty:
        logger.warning("engineer_features received empty DataFrame")
        return pd.DataFrame(), {}

    d = df.copy()
    encoders: dict = {}

    d["brand_origin"] = d["brand"].apply(_classify_origin)

    d["sentiment_polarisation"] = d.apply(_sentiment_polarisation, axis=1)
    d["crowd_pleaser_index"]    = d.apply(_crowd_pleaser, axis=1)
    d["season_dominant"]        = d.apply(_season_dominant, axis=1)
    d["note_count"]             = d.apply(_note_count, axis=1)

    current_year = dt.date.today().year
    d["years_on_market"] = d["year"].apply(
        lambda y: current_year - int(y) if pd.notna(y) else np.nan
    )

    if {"price_eur_usa", "price_eur_uae"}.issubset(d.columns):
        d["usa_uae_gap_pct"] = np.where(
            d["price_eur_uae"] > 0,
            (d["price_eur_usa"] - d["price_eur_uae"]) / d["price_eur_uae"],
            np.nan,
        )

    for col in ("gender", "brand_origin"):
        if col in d.columns:
            le = LabelEncoder()
            vals = d[col].fillna("unknown").astype(str)
            d[f"{col}_id"] = le.fit_transform(vals)
            encoders[f"label_{col}"] = le

    try:
        ohe_kwargs = {"sparse_output": False, "handle_unknown": "ignore"}
        OneHotEncoder(**ohe_kwargs)
    except TypeError:
        ohe_kwargs = {"sparse": False, "handle_unknown": "ignore"}

    for col, prefix in (("fragrance_family", "fam"), ("price_band", "band")):
        if col in d.columns:
            ohe = OneHotEncoder(**ohe_kwargs)
            vals = d[[col]].fillna("unknown").astype(str)
            mat = ohe.fit_transform(vals)
            cats = ohe.categories_[0]
            ohe_df = pd.DataFrame(
                mat,
                columns=[f"{prefix}_{c.lower().replace(' ', '_')}" for c in cats],
                index=d.index,
            )
            d = pd.concat([d, ohe_df], axis=1)
            encoders[f"onehot_{col}"] = ohe

    scale_cols = [c for c in ("rating", "price_per_ml", "votes")
                if c in d.columns]
    if scale_cols:
        scaler = MinMaxScaler()
        # scaler cannot accept NaN; temporarily fill with column mean
        tmp = d[scale_cols].copy()
        tmp = tmp.fillna(tmp.mean(numeric_only=True))
        scaled = scaler.fit_transform(tmp)
        for i, c in enumerate(scale_cols):
            d[f"{c}_scaled"] = scaled[:, i]
        encoders["minmax_numeric"] = scaler

    if "rating_scaled" in d.columns and "price_per_ml_eur_scaled" in d.columns:
        d["value_score"] = np.where(
            d["price_per_ml_eur_scaled"] > 0,
            d["rating_scaled"] / (d["price_per_ml_eur_scaled"] + 1e-6),
            np.nan,
        )

    ohe_cols = [c for c in d.columns if c.startswith(("fam_", "band_"))]
    if ohe_cols:
        vt = VarianceThreshold(threshold=0.01)
        try:
            vt.fit(d[ohe_cols].fillna(0))
            kept = [c for c, k in zip(ohe_cols, vt.get_support()) if k]
            dropped = set(ohe_cols) - set(kept)
            if dropped:
                logger.info("VarianceThreshold dropped %d near-constant cols",
                            len(dropped))
                d = d.drop(columns=list(dropped))
            encoders["variance_threshold"] = vt
        except Exception as e:
            logger.warning("VarianceThreshold skipped: %s", e)

    num_feat = [c for c in d.columns if c.endswith("_scaled")
                or c in ("value_score", "sentiment_polarisation",
                         "crowd_pleaser_index", "usa_uae_gap_pct",
                         "note_count", "years_on_market")]
    if len(num_feat) > 1:
        corr = d[num_feat].corr().abs()
        upper = corr.where(np.triu(np.ones(corr.shape), k=1).astype(bool))
        to_drop = [c for c in upper.columns if any(upper[c] > 0.95)]
        if to_drop:
            logger.info("Correlation filter dropped: %s", to_drop)
            d = d.drop(columns=to_drop)

    logger.info("Feature engineering complete: %d rows × %d cols",
                len(d), len(d.columns))
    return d, encoders
