# Fragrance Market Intelligence System

**B9AI001 Programming for Data Analysis - CA2**
**Student:** Daneel khan · **ID:** 20084451 · **MSc AI, Dublin Business School**

A daily-updating data acquisition and analysis pipeline for the global fragrance market. Scrapes 144 fragrances across ~29 brands from Fragrantica (community sentiment, notes, accords) and Amazon USA + UAE (live pricing), normalises everything to EUR using live exchange rates, engineers ML-ready features, loads the result into SQLite, and produces a self-contained HTML intelligence report - every morning at 06:00 UTC via GitHub Actions.

---

## What this project actually is

Fragrance market data is fragmented across incompatible sources. **Fragrantica** has community ratings, sentiment breakdowns (love/like/ok/dislike/hate), seasonal voting, note pyramids, and accord profiles - but no prices. **Amazon** (USA and UAE) has live pricing, stock status, and review counts - but no community sentiment and no consistent product taxonomy. Nobody combines them, and nobody tracks them daily.

This system does both. It scrapes both sources every day, joins them on `(name, brand)`, and produces:

- A **normalised SQLite database** with five tables and full referential integrity
- A **feature store** with one-hot encoded families, scaled price/rating, and four derived metrics (`value_score`, `sentiment_polarisation`, `crowd_pleaser_index`, `usa_uae_gap_pct`)
- **Four analytical CSVs** that answer real market questions: designer vs Arabic positioning, USA-vs-UAE arbitrage opportunities, value-for-money leaderboards, and sentiment polarisation buckets
- A **self-contained HTML report** with six matplotlib charts that drops on disk every morning, ready to open in any browser

The whole thing is real - it's the analytical backbone of *Opulence by Arkane*, my luxury fragrance brand. The first customer of this pipeline is me.

---

## Why it's useful

For a brand operator like me, the system answers questions I'd otherwise pay a market research firm for:

1. **"Which Arabic brands are pricing closest to designer?"** - Designer-vs-Arabic comparison shows where Lattafa and Armaf are encroaching on Dior and Chanel territory, segmented by accord family.
2. **"Where are the cross-market arbitrage opportunities?"** - The USA-vs-UAE arbitrage analysis flags any product whose EUR-normalised price differs by >25% between the two markets, sorted by gap percentage. Useful for sourcing decisions.
3. **"Which products give the best rating per euro?"** - The value-leaderboard combines Fragrantica's community rating with Amazon's price-per-ml to produce a `value_score`, ranked per brand-origin.
4. **"Which products are loved by everyone vs polarising?"** - Sentiment polarisation `(love − hate) / total_votes` distinguishes crowd-pleasers (Sauvage, Bleu de Chanel) from love-or-hate niche products. Useful for new launches: are you aiming for broad appeal or signature scent positioning?

For an academic context, the project demonstrates a complete end-to-end data pipeline: acquisition under real-world constraints (Cloudflare, bot detection, geo-IP issues), defensive cleaning, encoder-based feature engineering, normalised database design, daily automation, and reproducible analysis with full provenance.

---

## Project structure

Help from Gpt

https://chatgpt.com/share/69dfaf5b-68e4-832a-852a-b5ada01720b0

https://chatgpt.com/share/69ddb509-5340-838e-bfa7-0aa0fa19c97c

Took help of cursor for brainstorming, bugs and errors

```
fragrance-market-intelligence/
│
├── scrape_fragrantica.py       # Playwright + BeautifulSoup, fresh context per page
├── scrape_amazon.py            # Playwright + scoring matcher + price extractor
├── fx_rates.py                 # ECB live-fetch with per-day caching
├── transforms.py               # 14 cleaning/typing functions
├── feature_engineering.py      # Encoders + scalers + derived features
├── database.py                 # SQLite raw-data loader (4 tables)
├── database_features.py        # 5th table for the engineered feature store
├── analysis.py                 # Four analytical functions writing CSVs
├── report.py                   # Single-file HTML dashboard with embedded PNGs
├── daily_scraper.py            # Pipeline orchestrator (CLI entry point)
│
├── tests.py                    # Unit + integration tests for transforms & DB
├── tests_feature_engineering.py
│
├── products.json               # 144 fragrances with Fragrantica URLs + Amazon hints
├── fx_cache.json               # Per-day FX rates, committed to git for provenance
├── fragrance_market.db         # SQLite database (committed daily)
│
├── Makefile                    # make scrape | clean | features | analyze | report
├── requirements.txt
│
├── .github/workflows/
│   └── daily_scrape.yml        # 06:00 UTC cron + manual trigger options
│
├── raw_data/
│   ├── fragrantica_raw.csv     # Idempotent: same-day re-runs replace, not append
│   └── amazon_raw.csv
│
└── cleaned_data/
    ├── fragrantica_cleaned.csv
    ├── amazon_cleaned.csv
    ├── merged_normalized.csv
    ├── features.csv
    ├── analysis_designer_vs_arabic.csv
    ├── analysis_usa_vs_uae_arbitrage.csv
    ├── analysis_value_leaderboard.csv
    ├── analysis_sentiment_polarisation.csv
    └── report_YYYY-MM-DD.html
```

---

## Pipeline flow

The pipeline runs in five sequential stages, each with its own intermediate checkpoint on disk so any stage can be re-run independently for debugging.

```
┌─────────┐   ┌────────┐   ┌──────────┐   ┌────────┐   ┌─────────┐
│ SCRAPE  │ → │ CLEAN  │ → │ ENGINEER │ → │  LOAD  │ → │ ANALYSE │
└─────────┘   └────────┘   └──────────┘   └────────┘   └─────────┘
     │             │             │             │             │
     ▼             ▼             ▼             ▼             ▼
  raw_data/   cleaned_data/  features.csv  SQLite DB    analysis_*.csv
                                                        + report.html
```

**Stage 1 - Scrape.** `scrape_fragrantica.py` uses Playwright with a fresh browser context per product to defeat Cloudflare session tracking. It waits for the Vue-hydrated `.tw-rating-card div.flex-col.items-center` selector before parsing, then extracts ratings, votes, accords, perfumers, sentiment breakdowns, and seasonal voting from each product page. `scrape_amazon.py` uses Playwright + a card-scoring matcher that combines a non-fragrance blacklist, brand-token coverage, full-name boosts, and three-tier size matching to pick the right product variant from search results; the price extractor prefers the featured offer in `div[data-cy="price-recipe"]` over secondary "More Buying Choices" listings.

**Stage 2 - Clean.** `transforms.py` provides 14 functions that handle raw-string parsing (rating "4.7 out of 5 stars" → `4.7`), display-count parsing ("10.5k" → `10500`), price extraction across currency symbols, fl oz → ml conversion, and currency normalisation to EUR. **EUR conversion uses live ECB rates** fetched at scrape time and cached per-day in `fx_cache.json`, so every database row has a verifiable FX snapshot for the day it was created. Empty rows (no price AND no rating) are dropped lazily - partial signal is kept.

**Stage 3 - Engineer.** `feature_engineering.py` fits a `LabelEncoder` for gender and brand-origin (designer / arabic / niche, classified from a curated brand set), a `OneHotEncoder` for fragrance family and price band, and a `MinMaxScaler` for rating, price-per-ml, and votes. Derived features include `sentiment_polarisation`, `crowd_pleaser_index`, `value_score`, `note_count`, `years_on_market`, and `usa_uae_gap_pct`. A variance threshold filter and a correlation filter (|r| > 0.95) prune redundant features.

**Stage 4 - Load.** `database.py` writes raw cleaned data to four normalised tables (`fragrances`, `fragrantica_data`, `amazon_prices`, `notes_accords`) with foreign-key relations. `database_features.py` adds a fifth table (`fragrance_features`) keyed on `(fragrance_id, scrape_date)` UNIQUE, with dynamic `ALTER TABLE ADD COLUMN` for new one-hot columns as fragrance families appear over time. All writes use `INSERT OR REPLACE` so same-day re-runs are idempotent.

**Stage 5 - Analyse + Report.** `analysis.py` runs four focused analyses against the feature store, each producing a CSV in `cleaned_data/`. `report.py` reads from the SQLite feature table and renders six matplotlib charts (price distribution by brand origin, USA vs UAE arbitrage scatter, rating vs value-score, sentiment polarisation histogram, family composition, 30-day price trend) into a single self-contained HTML file with base64-embedded PNGs.

---

## Features at a glance

### Data acquisition
- **Two-source scraping** - Fragrantica + Amazon USA + Amazon UAE
- **Cloudflare bypass** - Playwright with fresh browser context per product page
- **Card-scoring matcher** - non-fragrance blacklist, brand-token coverage rule, size-aware ranking, tie-breaker by closest-size, dropout-aware extraction (skips the variant dropdown when reading sizes)
- **Geo-IP aware** - designed to run on US-based GitHub Actions runners to bypass the EUR redirect that hits Irish IPs on Amazon.com
- **Rate-limited** - 5–10 second per-product delay, 30–60 second pause every 20 products

### Data quality
- **Live FX rates** - ECB daily XML feed, cached per-day in git for provenance, three-tier fallback (exact-date → most recent earlier date - hardcoded)
- **Idempotent saves** - same-day re-runs replace existing rows by `(scrape_date, country, name, brand)` key, never duplicate
- **Defensive cleaning** - missing columns are added as None, empty rows are dropped only when both price AND rating are missing
- **Three-state size matching** - `match` / `conflict` / `unknown`, where unknown is treated as acceptable rather than rejection

### Analytics
- **Four canonical analyses** - designer vs Arabic, USA vs UAE arbitrage, value leaderboard, sentiment polarisation
- **HTML intelligence report** - six base64-embedded charts in a single file
- **53 unit tests + 1 integration test** - covering every transform function and a full pipeline smoke test

### Reproducibility
- **Daily GitHub Actions cron** - 06:00 UTC, US-based runner, commits raw + cleaned + DB + FX cache back to repo
- **Full provenance trail** - every database row has a `scrape_date` that maps to an FX rate in committed `fx_cache.json`
- **Layered checkpoints** - raw CSV > cleaned CSV > features CSV > SQLite, any stage rerunnable independently

---

## How to run

### One-time setup

```bash
git clone <repo-url>
cd fragrance-market-intelligence
pip install -r requirements.txt
python -m playwright install chromium
```

### Daily workflows (via Makefile)

```bash
make scrape          # full pipeline: scrape > clean > engineer > DB > analyse > report
make scrape-frag     # Fragrantica only (no pricing)
make scrape-amz      # Amazon USA + UAE only
make scrape-test     # tiny 3-product run for development
make clean           # re-run cleaning on existing raw_data without scraping
make features        # re-run feature engineering on existing cleaned data
make analyze         # re-run the four analyses on existing features
make report          # rebuild today's HTML dashboard
make test            # run the 53-test suite
```

### Direct CLI

```bash
python daily_scraper.py                      # full pipeline
python daily_scraper.py --fragrantica-only   # skip Amazon
python daily_scraper.py --amazon-only        # skip Fragrantica
python daily_scraper.py --limit 5            # first 5 products only
python daily_scraper.py --test               # run unit tests
```

### Automation

The pipeline runs automatically every morning at 06:00 UTC via `.github/workflows/daily_scrape.yml`. It can also be triggered manually from the GitHub Actions UI with four mode options: `fragrantica-only`, `full`, `amazon-only`, `test`. Each run commits `raw_data/`, `cleaned_data/`, `fragrance_market.db`, and `fx_cache.json` back to the repository, building a verifiable longitudinal dataset day by day.

---

## What insights you get

Once the pipeline has run for a few days, the analytical CSVs and the HTML report start telling a story. Some examples from real runs:

**Designer vs Arabic positioning.** The price-per-ml distribution shows designer brands cluster between €1.20 and €3.00/ml, while Arabic houses (Lattafa, Armaf, Al Haramain, Afnan) sit between €0.20 and €0.45/ml - roughly a 5–7× gap. But community ratings overlap heavily: top Arabic products score 4.1–4.4, just behind designer's 4.3–4.6. The strategic question this poses: if the rating gap is small but the price gap is huge, why isn't the Arabic share larger? The data suggests prestige and brand familiarity carry most of the price premium.

**USA vs UAE arbitrage.** A handful of products show consistent EUR-normalised gaps above 50%. Lattafa Khamrah, for example, retails for around €23 on Amazon UAE but €51 on Amazon USA - the same bottle, the same authentic seller, more than double the price. This is real arbitrage data a brand operator can use for sourcing decisions, and a researcher can use to study international price discrimination in luxury goods.

**Value-for-money leaderboard.** The `value_score = scaled_rating / scaled_price_per_ml` metric surfaces the products that punch above their weight. Designer winners are usually the entry-level Sauvage EDT and Bleu de Chanel; Arabic winners are Khamrah, Yara, and Asad. Niche houses (Creed, Parfums de Marly, Xerjoff) almost never win - they trade entirely on prestige.

**Sentiment polarisation.** The `(love − hate) / total_votes` metric divides the catalogue into three buckets: **crowd-pleasers** (everyone agrees it's good - Sauvage, Bleu, Aventus), **neutral** (decent but unexciting), and **polarising** (love-or-hate - most Tom Ford, most Maison Margiela, anything with prominent oud or animalic notes). Useful for new-launch positioning.

**Longitudinal price trends.** With daily snapshots accumulating in the database, the 30-day price trend line on the HTML report shows whether brands are quietly raising prices, running stealth discounts, or holding steady. After a few months this becomes the most valuable artifact in the system.

---

## Developer documentation

### Adding a new product

Edit `products.json`. Each entry needs:

```json
{
  "name": "Product Name",
  "brand": "Brand Name",
  "category": "designer",                                     // optional, ignored
  "fragrantica_url": "https://www.fragrantica.com/perfume/...",
  "amazon_search": "Brand Name Product Name 100ml",          // optional override
  "bottle_ml": 100,                                          // for size matching
  "size_alternatives": [50],                                 // optional fallback sizes
  "size_strict": true                                        // false to skip size check
}
```

The `category` field is intentionally ignored - brand origin (designer / arabic / niche) is classified from a curated brand set in `feature_engineering.py`, not from the per-product field, because raw category strings are too inconsistent across data sources.

### Tweaking the matcher

The Amazon matcher lives in `scrape_amazon.py` and works in three layers:

1. **Hard rejects** in `score_match` - `NON_FRAG_RE` blacklists roll-on, oil, diffuser, dupe, sample, decant, etc. `_is_bundle` catches 3+ fragrance line names in one card (multi-product dupe bundles).
2. **Token coverage** - every word of the product name (after stripping apostrophes and stopwords) must appear in the card's h2 title. Coverage below 45% is a hard reject.
3. **Size scoring** - `extract_all_ml_from_card` reads sizes only from the h2 title and the size badge (NOT the variation dropdown), producing match / conflict / unknown verdicts. Match adds +6, conflict subtracts -6, unknown adds -2.

When a product is matching the wrong variant, the first thing to check is the SCORE log lines. If the wrong card scores higher, the size scoring is probably reading from the dropdown - verify with `card.select_one('h2').get_text()` to see what the matcher actually sees.

### Tweaking the cleaner

`transforms.py` has two entry points: `clean_fragrantica(df)` and `clean_amazon(df)`. Both follow the same pattern:

1. Defensive column existence check (add as None if missing)
2. Per-column transforms via `df[col].apply(transform_function)`
3. `_drop_empty_*_rows` to remove rows with no useful signal
4. Final projection to a curated `keep` list

When adding a new field from the scraper, you need to: add the raw column to the `required_cols` list, add a transform call, add the cleaned column to the `keep` list, and (if it's numeric) add it to `engineer_features` so it makes it into the feature store.

### Tweaking the FX rates

`fx_rates.py` fetches from the ECB daily XML feed at `https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml`. AED is not in the ECB basket - it's derived via the USD peg at 1 USD = 3.6725 AED. Cache is written to `fx_cache.json` in the project root; commit this file alongside the scraped data so every database row has a verifiable rate snapshot.

If the ECB feed is unreachable, the resolution falls through three tiers: exact-date cache hit → most recent earlier cached date → hardcoded fallback (`FALLBACK_RATES`). The pipeline never hard-fails on a network blip, but if you see "No live or cached rates available" in the logs, that means the cache file was deleted and the network was down at the same time - re-run when the network is back.

### Tweaking the database schema

`database.py` defines the four raw tables. `database_features.py` defines the fifth (`fragrance_features`). Both use idempotent migrations: `CREATE TABLE IF NOT EXISTS` plus `ALTER TABLE ADD COLUMN` wrapped in try/except, so re-running on an existing database is always safe.

The feature table grows variable-width: when a new one-hot column appears in `features_df` (a new fragrance family, for example), `load_features` does an `ALTER TABLE` to add the missing column. Old rows get NULL for that column. This means the schema evolves with the data without needing migrations.

### Adding a new analysis

`analysis.py` exposes four functions and a `run_all(features_df, output_dir)` driver. To add a fifth: write a new function that takes `features_df` and returns a DataFrame, add it to `run_all`, and have `report.py` pick up the new CSV and render a chart from it. The chart goes into the HTML report automatically if you add it to the chart list at the top of `build_report`.

### Testing

```bash
make test                    # all 54 tests
python -m unittest tests     # transforms + DB integration only
python -m unittest tests_feature_engineering   # encoders + scalers
```

The integration test in `tests.py::TestIntegration` runs a tiny end-to-end pipeline on synthetic data - useful for catching breakage when refactoring transforms or the database loader. It should always pass before committing.

### Known constraints

- **Irish IP geo-block.** Amazon.com serves EUR prices and degraded results from Irish IPs. This is not a code bug - it's the reason the pipeline runs on US-based GitHub Actions runners. For local development, scrape Amazon UAE only or use a phone hotspot.
- **Fragrantica IP rate-limiting.** Repeated scraping from the same IP triggers Cloudflare. Fresh browser context per page mitigates this, but if you hit a hard block, wait an hour or switch networks.
- **Fragrance availability.** Some fragrances are not readially ab=vailable as they are exclusive and are sold only through boutique or orders.
