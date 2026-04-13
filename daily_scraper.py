"""
daily_scraper.py — Main pipeline orchestrator
"""

import os
import sys
import json
import logging
import argparse
import pandas as pd
from datetime import datetime, timezone

from scrape_fragrantica import scrape_all as scrape_fragrantica, save_raw as save_frag
from scrape_amazon import scrape_all as scrape_amazon, save_raw as save_amz
from transforms import clean_fragrantica, clean_amazon, merge_datasets
from database import load_to_database, create_database
from feature_engineering import engineer_features
from database_features import load_features
from analysis import run_all as run_analysis
from report import build_report

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("daily")

RAW_DIR   = "raw_data"
CLEAN_DIR = "cleaned_data"


def main():
    parser = argparse.ArgumentParser(
        description="Fragrance Market Intelligence — Daily Pipeline"
    )
    parser.add_argument("--fragrantica-only", action="store_true",
                        help="Scrape Fragrantica only")
    parser.add_argument("--amazon-only", action="store_true",
                        help="Scrape Amazon only")
    parser.add_argument("--limit", type=int, default=None,
                        help="Limit to first N products")
    parser.add_argument("--test", action="store_true",
                        help="Run tests instead of scraping")
    args = parser.parse_args()

    if args.test:
        import unittest
        loader = unittest.TestLoader()
        suite  = unittest.TestSuite()

        from tests import TestTransformations, TestIntegration
        suite.addTests(loader.loadTestsFromTestCase(TestTransformations))
        suite.addTests(loader.loadTestsFromTestCase(TestIntegration))

        try:
            from tests_feature_engineering import (
                TestHelpers, TestEngineerFeatures,
            )
            suite.addTests(loader.loadTestsFromTestCase(TestHelpers))
            suite.addTests(loader.loadTestsFromTestCase(TestEngineerFeatures))
        except ImportError:
            logger.warning("tests_feature_engineering.py not found — skipped")

        unittest.TextTestRunner(verbosity=2).run(suite)
        return

    os.makedirs(RAW_DIR,   exist_ok=True)
    os.makedirs(CLEAN_DIR, exist_ok=True)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    with open("products.json") as f:
        products = json.load(f)

    logger.info("=" * 55)
    logger.info("  FRAGRANCE MARKET INTELLIGENCE — DAILY SCRAPE")
    logger.info(f"  Date: {today} | Products: {len(products)}")
    logger.info("=" * 55)

    if not args.amazon_only:
        logger.info("Scraping Fragrantica...........")
        frag_data = scrape_fragrantica(products, today, limit=args.limit)
        if frag_data:
            save_frag(frag_data)

    if not args.fragrantica_only:
        logger.info("Scraping Amazon USA + UAE.............")
        amz_data = scrape_amazon(products, today, limit=args.limit)
        if amz_data:
            save_amz(amz_data)

    logger.info("Cleaning...........-")
    frag_path = os.path.join(RAW_DIR, "fragrantica_raw.csv")
    amz_path  = os.path.join(RAW_DIR, "amazon_raw.csv")

    if not os.path.exists(frag_path):
        logger.error(f"{frag_path} not found. Run Fragrantica scraper first.")
        return

    frag_df = clean_fragrantica(pd.read_csv(frag_path))
    frag_df.to_csv(os.path.join(CLEAN_DIR, "fragrantica_cleaned.csv"),
                   index=False, encoding="utf-8")

    if os.path.exists(amz_path):
        amz_df = clean_amazon(pd.read_csv(amz_path))
        amz_df.to_csv(os.path.join(CLEAN_DIR, "amazon_cleaned.csv"),
                      index=False, encoding="utf-8")
        merged = merge_datasets(frag_df, amz_df)
    else:
        amz_df = None
        merged = frag_df

    merged.to_csv(os.path.join(CLEAN_DIR, "merged_normalized.csv"),
                  index=False, encoding="utf-8")
    logger.info(f"Merged: {len(merged)} rows")

    logger.info("Feature engineering...........")
    features_df, encoders = engineer_features(merged)
    logger.info(
        f"Features: {len(features_df)} rows x {len(features_df.columns)} cols "
        f"({len(encoders)} encoders fitted)"
    )
    features_df.to_csv(os.path.join(CLEAN_DIR, "features.csv"),
                       index=False, encoding="utf-8")

    logger.info("Loading to SQLite............")

    load_to_database(frag_df, amz_df)


    conn = create_database()
    try:
        n_feat = load_features(conn, features_df, today)
        logger.info(f"Persisted {n_feat} feature rows for {today}")
    finally:
        conn.close()

    logger.info("Running analysis...........")
    run_analysis(features_df)

    logger.info("Building HTML report..............")
    try:
        report_path = build_report(today)
        if report_path:
            logger.info(f"Report ready: {report_path}")
    except Exception as e:
        logger.warning(f"Report build failed (non-fatal): {e}")

    logger.info("=" * 55)
    logger.info(f"  DONE! Merged rows: {len(merged)} | "
                f"Feature rows: {len(features_df)}")
    logger.info("=" * 55)


if __name__ == "__main__":
    main()
