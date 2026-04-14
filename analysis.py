"""
Four analytical views over the feature store
Each function consumes the engineered features DataFrame and returns
a DataFrame written to cleaned_data/ as analysis_<n>.csv.
"""

import os
import logging
import numpy as np
import pandas as pd

logger = logging.getLogger("analysis")


def analyze_designer_vs_arabic(features_df):
    """Compare price-per-ml and rating distributions by brand origin."""
    if "brand_origin" not in features_df.columns:
        logger.warning("designer_vs_arabic: brand_origin column missing")
        return pd.DataFrame()

    d = features_df.copy()
    d = d[d["price_per_ml"].notna() & d["rating"].notna()]
    if d.empty:
        return pd.DataFrame()

    summary = d.groupby("brand_origin").agg(
        n_products=("name", "nunique"),
        mean_price_per_ml=("price_per_ml", "mean"),
        median_price_per_ml=("price_per_ml", "median"),
        mean_rating=("rating", "mean"),
        median_rating=("rating", "median"),
    ).round(3).reset_index()

    return summary.sort_values("mean_price_per_ml", ascending=False)


def analyze_usa_vs_uae_arbitrage(features_df, threshold_pct=25.0):
    """Find products with >threshold_pct EUR-normalised gap between markets.

    Both prices are already in EUR (converted at the day's ECB rate inside
    transforms.py), so the comparison is currency-neutral.
    """
    needed = {"name", "brand", "country", "price_eur"}
    if not needed.issubset(features_df.columns):
        logger.warning("usa_vs_uae: missing required columns")
        return pd.DataFrame()

    d = features_df[features_df["price_eur"].notna()].copy()

    usa = (d[d["country"] == "USA"][["name", "brand", "price_eur"]]
           .drop_duplicates(["name", "brand"])
           .rename(columns={"price_eur": "price_eur_usa"}))
    uae = (d[d["country"] == "UAE"][["name", "brand", "price_eur"]]
           .drop_duplicates(["name", "brand"])
           .rename(columns={"price_eur": "price_eur_uae"}))

    comp = pd.merge(usa, uae, on=["name", "brand"], how="inner")
    if comp.empty:
        return pd.DataFrame()

    comp["abs_gap_eur"] = (comp["price_eur_usa"] - comp["price_eur_uae"]).abs().round(2)
    comp["abs_gap_pct"] = (
        comp["abs_gap_eur"]
        / comp[["price_eur_usa", "price_eur_uae"]].min(axis=1)
        * 100
    ).round(1)
    comp["cheaper_in"] = np.where(
        comp["price_eur_usa"] < comp["price_eur_uae"], "USA", "UAE"
    )

    flagged = comp[comp["abs_gap_pct"] >= threshold_pct].copy()
    return flagged.sort_values("abs_gap_pct", ascending=False)

def analyze_value_leaderboard(features_df, top_n=10):
    """Rank products by value_score = rating / price_per_ml, per brand origin.

    Computed on the fly because feature_engineering.py doesn't materialise
    value_score as a stored column.
    """
    needed = {"name", "brand", "rating", "price_per_ml", "brand_origin"}
    if not needed.issubset(features_df.columns):
        logger.warning("value_leaderboard: missing required columns")
        return pd.DataFrame()

    d = features_df.copy()
    d = d[d["rating"].notna() & d["price_per_ml"].notna() & (d["price_per_ml"] > 0)]
    if d.empty:
        return pd.DataFrame()

    d["value_score"] = (d["rating"] / d["price_per_ml"]).round(3)

    agg = d.groupby(["name", "brand", "brand_origin"]).agg(
        rating=("rating", "mean"),
        price_per_ml=("price_per_ml", "mean"),
        value_score=("value_score", "mean"),
    ).round(3).reset_index()

    leaderboard = (
        agg.sort_values(["brand_origin", "value_score"], ascending=[True, False])
           .groupby("brand_origin")
           .head(top_n)
           .reset_index(drop=True)
    )
    return leaderboard


def analyze_sentiment_polarisation(features_df):
    """Bucket products into crowd_pleaser / neutral / polarising."""
    if "sentiment_polarisation" not in features_df.columns:
        logger.warning("sentiment: sentiment_polarisation column missing")
        return pd.DataFrame()

    d = features_df.copy()
    d = d[d["sentiment_polarisation"].notna()]
    if d.empty:
        return pd.DataFrame()

    def _bucket(score):
        if score >= 0.5:
            return "crowd_pleaser"
        elif score >= 0.2:
            return "neutral"
        else:
            return "polarising"

    d["bucket"] = d["sentiment_polarisation"].apply(_bucket)

    summary = d.groupby("bucket").agg(
        n_products=("name", "nunique"),
        mean_polarisation=("sentiment_polarisation", "mean"),
        examples=("name", lambda s: ", ".join(sorted(set(s))[:5])),
    ).round(3).reset_index()

    order = {"crowd_pleaser": 0, "neutral": 1, "polarising": 2}
    summary["_order"] = summary["bucket"].map(order)
    return summary.sort_values("_order").drop(columns="_order")


def run_all(features_df, output_dir="cleaned_data"):
    """Run every analysis, write each result to a CSV, return dict."""
    os.makedirs(output_dir, exist_ok=True)
    results = {}

    runners = [
        ("designer_vs_arabic",    analyze_designer_vs_arabic),
        ("usa_vs_uae_arbitrage",  analyze_usa_vs_uae_arbitrage),
        ("value_leaderboard",     analyze_value_leaderboard),
        ("sentiment_polarisation", analyze_sentiment_polarisation),
    ]

    for name, fn in runners:
        try:
            df = fn(features_df)
        except Exception as e:
            logger.warning(f"{name} failed: {e}")
            continue

        if df is None or df.empty:
            logger.info(f"{name}: empty result, no CSV written")
            continue

        path = os.path.join(output_dir, f"analysis_{name}.csv")
        df.to_csv(path, index=False)
        logger.info(f"Wrote {path} ({len(df)} rows)")
        results[name] = df

    return results
