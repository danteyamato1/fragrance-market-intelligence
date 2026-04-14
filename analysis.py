"""
Each function takes the feature-engineered DataFrame and returns a
small result DataFrame suitable for display or CSV export.
"""

from __future__ import annotations

import logging
import pandas as pd

logger = logging.getLogger("analysis")


def analyze_designer_vs_arabic(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compare median price-per-ml (EUR) between designer and arabic brands.
    Relies on the `brand_origin` feature added in feature_engineering.
    """
    if "brand_origin" not in df.columns or "price_per_ml_eur" not in df.columns:
        return pd.DataFrame()

    g = (
        df.dropna(subset=["price_per_ml_eur"])
          .groupby("brand_origin")["price_per_ml_eur"]
          .agg(["count", "median", "mean", "std"])
          .round(3)
          .reset_index()
    )
    # Premium multiplier vs arabic baseline
    if "arabic" in g["brand_origin"].values:
        base = g.loc[g["brand_origin"] == "arabic", "median"].iloc[0]
        if base > 0:
            g["premium_vs_arabic"] = (g["median"] / base).round(2)
    return g


def analyze_usa_vs_uae_arbitrage(df: pd.DataFrame,
                                 threshold: float = 0.25) -> pd.DataFrame:
    """
    Flag products where |usa_uae_gap_pct| exceeds `threshold`.

    A positive gap means USA is more expensive than UAE (source in UAE,
    sell in USA); a negative gap means the reverse. Directly actionable
    for the student's Opulence by Arkane sourcing decisions.
    """
    if "usa_uae_gap_pct" not in df.columns:
        return pd.DataFrame()

    d = df.dropna(subset=["usa_uae_gap_pct"]).copy()
    d["direction"] = d["usa_uae_gap_pct"].apply(
        lambda g: "buy_UAE_sell_USA" if g > 0 else "buy_USA_sell_UAE"
    )
    d["abs_gap_pct"] = (d["usa_uae_gap_pct"].abs() * 100).round(1)
    flagged = d[d["usa_uae_gap_pct"].abs() >= threshold]

    cols = [c for c in
            ("brand", "name", "price_eur_usa", "price_eur_uae",
             "abs_gap_pct", "direction")
            if c in flagged.columns]
    return (flagged[cols]
            .sort_values("abs_gap_pct", ascending=False)
            .reset_index(drop=True))


def analyze_value_leaderboard(df: pd.DataFrame,
                              top_n: int = 10) -> pd.DataFrame:
    """
    Rank products by the engineered `value_score` (rating / price-per-ml,
    both MinMax-scaled in feature_engineering). Returns top N per
    brand_origin so designer and arabic winners are both visible.
    """
    if "value_score" not in df.columns:
        return pd.DataFrame()

    d = df.dropna(subset=["value_score"]).copy()
    if "brand_origin" in d.columns:
        d = (d.sort_values("value_score", ascending=False)
              .groupby("brand_origin", group_keys=False)
              .head(top_n))
    else:
        d = d.nlargest(top_n, "value_score")

    cols = [c for c in
            ("brand_origin", "brand", "name", "rating",
             "price_per_ml_eur", "value_score")
            if c in d.columns]
    return d[cols].reset_index(drop=True)


def analyze_sentiment_polarisation(df: pd.DataFrame) -> pd.DataFrame:
    """
    Identify love-it-or-hate-it scents.

    A low |polarisation| with a high crowd_pleaser_index means a broadly
    liked fragrance; a low crowd_pleaser_index with high vote volume
    indicates a polarising scent — useful intelligence for a brand
    deciding which archetypes to target.
    """
    needed = {"sentiment_polarisation", "crowd_pleaser_index"}
    if not needed.issubset(df.columns):
        return pd.DataFrame()

    d = df.dropna(subset=list(needed)).copy()

    def _label(row):
        if row["crowd_pleaser_index"] >= 0.70:
            return "crowd_pleaser"
        if row["crowd_pleaser_index"] <= 0.40:
            return "polarising"
        return "neutral"

    d["profile"] = d.apply(_label, axis=1)

    summary = (
        d.groupby("profile")
         .agg(count=("name", "count"),
              mean_rating=("rating", "mean"),
              mean_polarisation=("sentiment_polarisation", "mean"))
         .round(3)
         .reset_index()
    )
    return summary


def run_all(features_df: pd.DataFrame,
            output_dir: str = "cleaned_data") -> dict:
    """Run the four analyses and write each result to CSV."""
    import os
    os.makedirs(output_dir, exist_ok=True)

    results = {
        "designer_vs_arabic":      analyze_designer_vs_arabic(features_df),
        "usa_vs_uae_arbitrage":    analyze_usa_vs_uae_arbitrage(features_df),
        "value_leaderboard":       analyze_value_leaderboard(features_df),
        "sentiment_polarisation":  analyze_sentiment_polarisation(features_df),
    }
    for name, res in results.items():
        if isinstance(res, pd.DataFrame) and not res.empty:
            path = os.path.join(output_dir, f"analysis_{name}.csv")
            res.to_csv(path, index=False, encoding="utf-8-sig")
            logger.info("Wrote %s (%d rows)", path, len(res))
    return results
