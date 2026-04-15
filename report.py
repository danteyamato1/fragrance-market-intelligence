"""
Daily HTML Intelligence Report (interactive edition)
"""

from __future__ import annotations
import os
import sys
import base64
import sqlite3
import logging
import datetime as dt
from io import BytesIO
from textwrap import dedent

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

logger = logging.getLogger("report")

try:
    from bokeh.plotting import figure
    from bokeh.embed import components
    from bokeh.resources import INLINE
    from bokeh.models import HoverTool, ColumnDataSource
    BOKEH_AVAILABLE = True
except ImportError:
    BOKEH_AVAILABLE = False
    logger.warning("bokeh not installed - interactive charts will fall back to matplotlib")

DB_PATH      = "fragrance_market.db"
OUTPUT_DIR   = "cleaned_data"
FEATURES_CSV = os.path.join(OUTPUT_DIR, "features.csv")

BRAND_COLORS = {
    "designer": "#3b82f6",
    "arabic":   "#10b981",
    "niche":    "#ef4444",
    "unknown":  "#9ca3af",
}

SENTIMENT_COLORS = {
    "crowd_pleaser": "#10b981",
    "neutral":       "#f59e0b",
    "polarising":    "#ef4444",
}


def _load_features(date: str | None = None) -> pd.DataFrame:
    if os.path.exists(FEATURES_CSV):
        try:
            df = pd.read_csv(FEATURES_CSV)
            if not df.empty:
                logger.info("Loaded features from %s (%d rows)", FEATURES_CSV, len(df))
                return df
        except Exception as e:
            logger.warning("Could not read %s: %s", FEATURES_CSV, e)

    if os.path.exists(DB_PATH):
        try:
            conn = sqlite3.connect(DB_PATH)
            try:
                date = date or dt.date.today().isoformat()
                q = """
                SELECT f.*, fr.name, b.brand_name AS brand
                FROM fragrance_features f
                JOIN fragrances fr ON f.fragrance_id = fr.fragrance_id
                JOIN brands     b  ON fr.brand_id    = b.brand_id
                WHERE f.scrape_date = ?
                """
                df = pd.read_sql_query(q, conn, params=(date,))
                if not df.empty:
                    return df
            finally:
                conn.close()
        except Exception as e:
            logger.warning("SQLite fallback failed: %s", e)

    merged_path = os.path.join(OUTPUT_DIR, "merged_normalized.csv")
    if os.path.exists(merged_path):
        try:
            from feature_engineering import engineer_features
            merged = pd.read_csv(merged_path)
            feats, _ = engineer_features(merged)
            return feats
        except Exception as e:
            logger.warning("Re-engineer fallback failed: %s", e)

    return pd.DataFrame()


def _load_history(days: int = 30) -> pd.DataFrame:
    if not os.path.exists(DB_PATH):
        return pd.DataFrame()
    try:
        conn = sqlite3.connect(DB_PATH)
        try:
            q = """
            SELECT scrape_date, country,
                   AVG(price_per_ml) AS mean_ppm,
                   COUNT(*)          AS n
            FROM amazon_prices
            WHERE price_per_ml IS NOT NULL
              AND scrape_date >= date('now', ?)
            GROUP BY scrape_date, country
            ORDER BY scrape_date
            """
            return pd.read_sql_query(q, conn, params=(f"-{days} days",))
        finally:
            conn.close()
    except Exception as e:
        logger.warning("History query failed: %s", e)
        return pd.DataFrame()


def _build_arbitrage_pivot(df: pd.DataFrame) -> pd.DataFrame:
    needed = {"name", "brand", "country", "price_eur"}
    if not needed.issubset(df.columns):
        return pd.DataFrame()

    d = df[df["price_eur"].notna()].copy()
    bo_col = ["brand_origin"] if "brand_origin" in d.columns else []
    usa_cols = ["name", "brand", "price_eur"] + bo_col
    usa = (d[d["country"] == "USA"][usa_cols]
           .drop_duplicates(["name", "brand"])
           .rename(columns={"price_eur": "price_eur_usa"}))
    uae = (d[d["country"] == "UAE"][["name", "brand", "price_eur"]]
           .drop_duplicates(["name", "brand"])
           .rename(columns={"price_eur": "price_eur_uae"}))

    merged = pd.merge(usa, uae, on=["name", "brand"], how="inner")
    if not merged.empty:
        merged["gap_pct"] = (
            (merged["price_eur_usa"] - merged["price_eur_uae"]).abs()
            / merged[["price_eur_usa", "price_eur_uae"]].min(axis=1) * 100
        ).round(1)
        merged["cheaper_in"] = np.where(
            merged["price_eur_usa"] < merged["price_eur_uae"], "USA", "UAE"
        )
    return merged


def _add_value_score(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    if "rating" not in d.columns or "price_per_ml" not in d.columns:
        d["value_score"] = np.nan
        return d
    mask = d["rating"].notna() & d["price_per_ml"].notna() & (d["price_per_ml"] > 0)
    d["value_score"] = np.where(mask, d["rating"] / d["price_per_ml"], np.nan)
    return d


def _fig_to_b64(fig) -> str:
    buf = BytesIO()
    fig.savefig(buf, format="png", dpi=110, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return base64.b64encode(buf.getvalue()).decode("ascii")


def _style(ax, title: str):
    ax.set_title(title, fontsize=14, fontweight="bold", pad=14)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.tick_params(labelsize=10)
    ax.grid(axis="y", alpha=0.3, linestyle="--")


def chart_price_by_origin(df: pd.DataFrame) -> dict | None:
    if "brand_origin" not in df.columns or "price_per_ml" not in df.columns:
        return None

    d = df.dropna(subset=["price_per_ml", "brand_origin"])
    if d.empty:
        return None

    available = [o for o in ["arabic", "designer", "niche"]
                 if o in d["brand_origin"].unique()]
    if not available:
        return None

    data = [d[d["brand_origin"] == o]["price_per_ml"].values for o in available]
    colors = [BRAND_COLORS[o] for o in available]

    fig, ax = plt.subplots(figsize=(9, 5))
    bp = ax.boxplot(
        data,
        labels=[o.title() for o in available],
        patch_artist=True,
        medianprops=dict(color="black", linewidth=2.5),
        widths=0.6,
        showmeans=True,
        meanprops=dict(marker="D", markerfacecolor="white",
                       markeredgecolor="black", markersize=7),
    )
    for patch, c in zip(bp["boxes"], colors):
        patch.set_facecolor(c)
        patch.set_alpha(0.75)

    for i, vals in enumerate(data, start=1):
        median_val = np.median(vals)
        n = len(vals)
        ax.annotate(
            f"median\n€{median_val:.2f}/ml\n(n={n})",
            xy=(i, median_val),
            xytext=(i + 0.32, median_val),
            fontsize=9, color="#334155", va="center",
        )

    ax.set_ylabel("Price per millilitre (EUR)", fontsize=12)
    ax.set_xlabel("Brand origin", fontsize=12)
    _style(ax, "Price-per-ml distribution by brand origin")

    from matplotlib.lines import Line2D
    ax.legend(
        handles=[Line2D([0], [0], marker="D", color="w", label="Mean",
                        markerfacecolor="white", markeredgecolor="black",
                        markersize=7)],
        loc="upper right", frameon=False, fontsize=10,
    )

    return {"type": "image", "content": _fig_to_b64(fig)}


def chart_usa_vs_uae_bokeh(arb_df: pd.DataFrame) -> dict | None:
    if arb_df.empty or "price_eur_usa" not in arb_df.columns:
        return None

    d = arb_df.dropna(subset=["price_eur_usa", "price_eur_uae"])
    if d.empty:
        return None

    if not BOKEH_AVAILABLE:
        return _fallback_usa_uae(d)

    d = d.copy()
    if "brand_origin" not in d.columns:
        d["brand_origin"] = "unknown"
    d["color"] = d["brand_origin"].map(BRAND_COLORS).fillna("#9ca3af")

    source = ColumnDataSource(data=dict(
        x=d["price_eur_uae"].tolist(),
        y=d["price_eur_usa"].tolist(),
        name=d["name"].tolist(),
        brand=d["brand"].tolist(),
        origin=d["brand_origin"].tolist(),
        gap=d["gap_pct"].tolist() if "gap_pct" in d.columns else [0] * len(d),
        cheaper=d["cheaper_in"].tolist() if "cheaper_in" in d.columns else [""] * len(d),
        color=d["color"].tolist(),
    ))

    mx = max(d["price_eur_usa"].max(), d["price_eur_uae"].max()) * 1.1

    p = figure(
        width=780, height=480,
        title="USA vs UAE pricing — hover any dot for product details",
        x_axis_label="UAE price (EUR)",
        y_axis_label="USA price (EUR)",
        tools="pan,wheel_zoom,box_zoom,reset,save",
        x_range=(0, mx), y_range=(0, mx),
        toolbar_location="above",
        background_fill_color="#fafafa",
    )

    p.line([0, mx], [0, mx], line_dash="dashed",
           color="#64748b", line_width=1.5, alpha=0.6)

    p.scatter(
        "x", "y", source=source,
        size=13, color="color",
        fill_alpha=0.75, line_color="white", line_width=1.5,
    )

    hover = HoverTool(tooltips=[
        ("Brand",   "@brand"),
        ("Product", "@name"),
        ("Origin",  "@origin"),
        ("UAE",     "€@x{0,0.00}"),
        ("USA",     "€@y{0,0.00}"),
        ("Gap",     "@gap{0.0}% (cheaper in @cheaper)"),
    ])
    p.add_tools(hover)

    p.title.text_font_size = "14pt"
    p.title.text_font_style = "bold"
    p.xaxis.axis_label_text_font_size = "11pt"
    p.yaxis.axis_label_text_font_size = "11pt"
    p.grid.grid_line_alpha = 0.3
    p.outline_line_color = None

    script, div = components(p)
    return {"type": "bokeh", "script": script, "div": div}


def _fallback_usa_uae(d: pd.DataFrame) -> dict:
    fig, ax = plt.subplots(figsize=(9, 5.5))
    colors = [BRAND_COLORS.get(o, "#9ca3af")
              for o in d.get("brand_origin", ["unknown"] * len(d))]
    ax.scatter(d["price_eur_uae"], d["price_eur_usa"],
               c=colors, s=70, alpha=0.75, edgecolor="white", linewidth=1)
    mx = max(d["price_eur_usa"].max(), d["price_eur_uae"].max()) * 1.1
    ax.plot([0, mx], [0, mx], "k--", alpha=0.4, linewidth=1.2)
    ax.set_xlim(0, mx); ax.set_ylim(0, mx)
    ax.set_xlabel("UAE price (EUR)", fontsize=12)
    ax.set_ylabel("USA price (EUR)", fontsize=12)
    _style(ax, "USA vs UAE pricing")
    return {"type": "image", "content": _fig_to_b64(fig)}


def chart_rating_vs_value_bokeh(df: pd.DataFrame) -> dict | None:
    if "rating" not in df.columns or "value_score" not in df.columns:
        return None

    d = df.dropna(subset=["rating", "value_score"])
    if d.empty:
        return None

    bo_col = "brand_origin" if "brand_origin" in d.columns else None
    agg = {"rating": ("rating", "mean"),
           "value_score": ("value_score", "mean"),
           "price_per_ml": ("price_per_ml", "mean")}
    if bo_col:
        agg["brand_origin"] = (bo_col, "first")
    d = d.groupby(["name", "brand"]).agg(**agg).reset_index()

    if not BOKEH_AVAILABLE:
        return _fallback_rating_value(d)

    if "brand_origin" not in d.columns:
        d["brand_origin"] = "unknown"
    d["color"] = d["brand_origin"].map(BRAND_COLORS).fillna("#9ca3af")

    source = ColumnDataSource(data=dict(
        x=d["value_score"].tolist(),
        y=d["rating"].tolist(),
        name=d["name"].tolist(),
        brand=d["brand"].tolist(),
        origin=d["brand_origin"].tolist(),
        ppm=d["price_per_ml"].tolist(),
        color=d["color"].tolist(),
    ))

    p = figure(
        width=780, height=480,
        title="Rating vs value for money — top-right is best",
        x_axis_label="Value score (rating ÷ price per ml)",
        y_axis_label="Fragrantica rating",
        tools="pan,wheel_zoom,box_zoom,reset,save",
        toolbar_location="above",
        background_fill_color="#fafafa",
    )

    p.scatter(
        "x", "y", source=source,
        size=14, color="color",
        fill_alpha=0.75, line_color="white", line_width=1.5,
    )

    hover = HoverTool(tooltips=[
        ("Brand",    "@brand"),
        ("Product",  "@name"),
        ("Origin",   "@origin"),
        ("Rating",   "@y{0.00} / 5"),
        ("€/ml",     "€@ppm{0.00}"),
        ("Value",    "@x{0.000}"),
    ])
    p.add_tools(hover)

    p.title.text_font_size = "14pt"
    p.title.text_font_style = "bold"
    p.xaxis.axis_label_text_font_size = "11pt"
    p.yaxis.axis_label_text_font_size = "11pt"
    p.grid.grid_line_alpha = 0.3
    p.outline_line_color = None

    script, div = components(p)
    return {"type": "bokeh", "script": script, "div": div}


def _fallback_rating_value(d: pd.DataFrame) -> dict:
    fig, ax = plt.subplots(figsize=(9, 5.5))
    colors = [BRAND_COLORS.get(o, "#9ca3af")
              for o in d.get("brand_origin", ["unknown"] * len(d))]
    ax.scatter(d["value_score"], d["rating"],
               c=colors, s=70, alpha=0.75, edgecolor="white", linewidth=1)
    top3 = d.nlargest(3, "value_score")
    for _, row in top3.iterrows():
        ax.annotate(f"{row['brand']} {row['name']}"[:30],
                    (row["value_score"], row["rating"]),
                    fontsize=9, xytext=(5, 5), textcoords="offset points")
    ax.set_xlabel("Value score (rating ÷ price per ml)", fontsize=12)
    ax.set_ylabel("Fragrantica rating", fontsize=12)
    _style(ax, "Rating vs value for money — top-right = best")
    return {"type": "image", "content": _fig_to_b64(fig)}


def chart_sentiment_donut(df: pd.DataFrame) -> dict | None:
    if "sentiment_polarisation" not in df.columns:
        return None

    d = df.dropna(subset=["sentiment_polarisation"]).copy()
    if d.empty:
        return None

    if "crowd_pleaser_index" in d.columns and d["crowd_pleaser_index"].notna().any():
        def _bucket(row):
            idx = row["crowd_pleaser_index"]
            if pd.isna(idx):   return "neutral"
            if idx >= 0.70:    return "crowd_pleaser"
            if idx <= 0.40:    return "polarising"
            return "neutral"
        d["bucket"] = d.apply(_bucket, axis=1)
    else:
        def _simple(score):
            if score >= 0.5:  return "crowd_pleaser"
            if score >= 0.2:  return "neutral"
            return "polarising"
        d["bucket"] = d["sentiment_polarisation"].apply(_simple)

    d = d.drop_duplicates(subset=["name", "brand"])

    counts = d["bucket"].value_counts()
    order = ["crowd_pleaser", "neutral", "polarising"]
    counts = counts.reindex([o for o in order if o in counts.index])

    labels = [o.replace("_", " ").title() for o in counts.index]
    colors = [SENTIMENT_COLORS[o] for o in counts.index]

    fig, ax = plt.subplots(figsize=(9, 5.5))
    wedges, texts, autotexts = ax.pie(
        counts.values,
        labels=labels,
        autopct=lambda pct: f"{pct:.0f}%\n({int(round(pct * counts.sum() / 100))})",
        startangle=90,
        colors=colors,
        pctdistance=0.78,
        wedgeprops=dict(width=0.45, edgecolor="white", linewidth=3),
    )
    for t in texts:
        t.set_fontsize(12); t.set_fontweight("bold")
    for t in autotexts:
        t.set_fontsize(10); t.set_color("white"); t.set_fontweight("bold")

    ax.set_title("Sentiment profile distribution",
                 fontsize=14, fontweight="bold", pad=20)
    ax.text(0, 0, f"{counts.sum()}\nproducts",
            ha="center", va="center",
            fontsize=14, fontweight="bold", color="#334155")

    return {"type": "image", "content": _fig_to_b64(fig)}


def chart_family_donut(df: pd.DataFrame) -> dict | None:
    if "fragrance_family" not in df.columns:
        return None
    d = df[df["fragrance_family"].notna() & (df["fragrance_family"] != "")].copy()
    if d.empty:
        return None

    d = d.drop_duplicates(subset=["name", "brand"])
    counts = d["fragrance_family"].value_counts()

    TOP = 8
    if len(counts) > TOP:
        top = counts.head(TOP)
        other = counts.iloc[TOP:].sum()
        counts = pd.concat([top, pd.Series({"Other": other})])

    colors = plt.cm.Set3(np.linspace(0.1, 0.95, len(counts)))

    fig, ax = plt.subplots(figsize=(10, 6))
    wedges, texts, autotexts = ax.pie(
        counts.values,
        labels=counts.index,
        autopct=lambda pct: f"{pct:.0f}%" if pct >= 4 else "",
        startangle=90,
        colors=colors,
        pctdistance=0.78,
        wedgeprops=dict(width=0.45, edgecolor="white", linewidth=2),
    )
    for t in texts:
        t.set_fontsize(10)
    for t in autotexts:
        t.set_fontsize(9); t.set_fontweight("bold"); t.set_color("#1e293b")

    ax.set_title(f"Fragrance family composition (top {TOP})",
                 fontsize=14, fontweight="bold", pad=20)
    ax.text(0, 0, f"{int(counts.sum())}\nproducts",
            ha="center", va="center",
            fontsize=14, fontweight="bold", color="#334155")

    return {"type": "image", "content": _fig_to_b64(fig)}


def chart_price_history(hist: pd.DataFrame) -> dict | None:
    if hist.empty or "scrape_date" not in hist.columns:
        return None
    if hist["scrape_date"].nunique() < 2:
        return None  # line needs at least 2 days

    fig, ax = plt.subplots(figsize=(9, 5))
    country_colors = {"USA": "#3b82f6", "UAE": "#10b981"}
    for country, grp in hist.groupby("country"):
        ax.plot(grp["scrape_date"], grp["mean_ppm"],
                marker="o", label=country, linewidth=2.5,
                color=country_colors.get(country, "#64748b"))

    ax.set_xlabel("Scrape date", fontsize=12)
    ax.set_ylabel("Mean price per ml (EUR)", fontsize=12)
    ax.legend(frameon=False, fontsize=11)
    plt.xticks(rotation=35, ha="right")
    _style(ax, "Mean price-per-ml over time")
    return {"type": "image", "content": _fig_to_b64(fig)}


_TEMPLATE = dedent("""\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Fragrance Market Intelligence — {date}</title>
{bokeh_head}
<style>
  body {{
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    background: #f8fafc; color: #1e293b;
    margin: 0; padding: 40px 20px;
  }}
  .container {{ max-width: 1020px; margin: 0 auto; }}
  header {{ border-bottom: 3px solid #3b82f6; padding-bottom: 16px; margin-bottom: 32px; }}
  h1 {{ margin: 0 0 8px; font-size: 30px; }}
  .subtitle {{ color: #64748b; font-size: 14px; }}
  .meta {{ display: flex; gap: 32px; margin-top: 20px; font-size: 13px; color: #475569; }}
  .meta strong {{ display: block; font-size: 22px; color: #0f172a; margin-bottom: 2px; }}
  .chart {{
    background: white; border-radius: 12px; padding: 28px;
    margin-bottom: 24px; box-shadow: 0 2px 8px rgba(0,0,0,0.06);
  }}
  .chart img {{ max-width: 100%; height: auto; display: block; margin: 0 auto; }}
  .chart h2 {{
    font-size: 13px; color: #64748b; margin: 0 0 18px;
    text-transform: uppercase; letter-spacing: 1px;
  }}
  .chart .bk-root {{ margin: 0 auto; }}
  .narrative {{
    font-size: 14px; color: #475569;
    margin-top: 14px; line-height: 1.6;
    padding: 12px 16px;
    background: #f8fafc; border-left: 3px solid #3b82f6;
    border-radius: 4px;
  }}
  .interactive-badge {{
    display: inline-block;
    background: #dbeafe; color: #1e40af;
    font-size: 10px; font-weight: 700;
    padding: 3px 8px; border-radius: 10px;
    margin-left: 8px; letter-spacing: 0.5px;
    vertical-align: middle;
  }}
  footer {{ text-align: center; color: #94a3b8; font-size: 12px; margin-top: 40px; }}
</style>
</head>
<body>
<div class="container">

<header>
  <h1>Fragrance Market Intelligence</h1>
  <div class="subtitle">Daily report &middot; {date}</div>
  <div class="meta">
    <div><strong>{n_products}</strong> products tracked</div>
    <div><strong>{n_priced}</strong> with pricing</div>
    <div><strong>{n_rated}</strong> with rating</div>
  </div>
</header>

{charts_html}

<footer>
  Generated by the Fragrance Market Intelligence pipeline &middot;
  Sources: Fragrantica, Amazon.com, Amazon.ae &middot; FX rates: ECB
</footer>

</div>
</body>
</html>
""")


def _chart_block(title: str, chart: dict | None, narrative: str = "") -> str:
    if not chart:
        return ""
    badge = '<span class="interactive-badge">INTERACTIVE</span>' \
            if chart["type"] == "bokeh" else ""
    narr = f'<p class="narrative">{narrative}</p>' if narrative else ""
    if chart["type"] == "image":
        body = f'<img src="data:image/png;base64,{chart["content"]}" alt="{title}">'
    else:
        body = chart["div"] + "\n" + chart["script"]
    return dedent(f"""\
        <div class="chart">
          <h2>{title}{badge}</h2>
          {body}
          {narr}
        </div>
    """)


def build_report(date: str | None = None,
                 db_path: str = DB_PATH,
                 output_dir: str = OUTPUT_DIR) -> str | None:
    date = date or dt.date.today().isoformat()
    os.makedirs(output_dir, exist_ok=True)

    df = _load_features(date)
    if df.empty:
        logger.warning("No feature data available — nothing to report")
        return None

    logger.info("Loaded %d feature rows", len(df))

    arb_df = _build_arbitrage_pivot(df)
    df = _add_value_score(df)
    hist = _load_history(days=30)

    n_products = df[["name", "brand"]].drop_duplicates().shape[0] \
                 if {"name", "brand"}.issubset(df.columns) else len(df)
    n_priced = int(df["price_eur"].notna().sum()) if "price_eur" in df.columns else 0
    n_rated  = int(df["rating"].notna().sum())    if "rating"    in df.columns else 0

    charts = [
        ("Price distribution by brand origin",
         chart_price_by_origin(df),
         "The box shows the middle 50% of prices, the line inside is the median, "
         "the diamond is the mean. Niche houses sit at the top of the range; Arabic "
         "brands cluster 5–10× cheaper per ml."),

        ("USA vs UAE arbitrage",
         chart_usa_vs_uae_bokeh(arb_df),
         "Hover any dot to see the product name, both prices, and the arbitrage gap. "
         "Dots above the parity line are cheaper in UAE — potential sourcing "
         "opportunities. Dots below are cheaper in USA."),

        ("Rating vs value for money",
         chart_rating_vs_value_bokeh(df),
         "Hover to identify any product. Top-right quadrant = highly rated AND good "
         "value. Upper-left dots (high rating, low value) are prestige plays — "
         "you pay for the name, not the scent."),

        ("Sentiment polarisation",
         chart_sentiment_donut(df),
         "Crowd-pleasers (broadly loved) are safer launches. Polarising scents can "
         "still succeed as signature fragrances for a narrow audience — but they "
         "carry more market risk."),

        ("Fragrance family composition",
         chart_family_donut(df),
         "Distribution of fragrance families across the tracked catalogue. "
         "Used as one-hot encoded features in the ML-ready feature store."),

        ("Daily price trend",
         chart_price_history(hist),
         "Mean price-per-ml over the last 30 days. Requires multiple days of "
         "scraped data - intentionally omitted on a single-day dataset."),
    ]

    rendered = [(t, c, n) for t, c, n in charts if c]
    logger.info("Rendered %d/%d charts", len(rendered), len(charts))
    for t, c, _ in charts:
        if c:
            tag = "bokeh" if c["type"] == "bokeh" else "static"
            logger.info("  [OK] %s  (%s)", t, tag)
        else:
            logger.info("  [--] %s  (skipped)", t)

    charts_html = "\n".join(_chart_block(t, c, n) for t, c, n in rendered)

    has_bokeh = any(c["type"] == "bokeh" for _, c, _ in rendered)
    bokeh_head = INLINE.render() if (has_bokeh and BOKEH_AVAILABLE) else ""

    html = _TEMPLATE.format(
        date=date,
        bokeh_head=bokeh_head,
        n_products=n_products,
        n_priced=n_priced,
        n_rated=n_rated,
        charts_html=charts_html,
    )

    output_path = os.path.join(output_dir, f"report_{date}.html")
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)

    logger.info("Report written: %s", output_path)
    return output_path


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    date = sys.argv[1] if len(sys.argv) > 1 else None
    path = build_report(date)
    if path:
        print(f"Report built: {path}")
    else:
        print("Nothing to report")
        sys.exit(1)
