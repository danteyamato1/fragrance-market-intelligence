"""
tests.py — Unit tests + integration test
==========================================
Unit tests: one per transformation function (edge cases + happy paths)
Integration test: raw data -> transform -> SQLite -> query -> verify
"""

import os, sqlite3, unittest
import pandas as pd
from transforms import (transform_rating, transform_votes, transform_year,
    transform_price, transform_bottle_size, convert_to_eur, compute_price_per_ml,
    classify_price_band, transform_amazon_rating, clean_fragrantica, clean_amazon)
from database import load_to_database


class TestTransformations(unittest.TestCase):
    """Unit tests for all transformation functions."""

    # transform_rating
    def test_rating_valid(self):       self.assertEqual(transform_rating("4.07"), 4.07)
    def test_rating_int(self):         self.assertEqual(transform_rating("4"), 4.0)
    def test_rating_none(self):        self.assertIsNone(transform_rating(None))
    def test_rating_out_of_range(self): self.assertIsNone(transform_rating("6.5"))
    def test_rating_text(self):        self.assertIsNone(transform_rating("excellent"))
    def test_rating_zero(self):        self.assertEqual(transform_rating("0"), 0.0)

    # transform_votes
    def test_votes_comma(self):    self.assertEqual(transform_votes("15,234"), 15234)
    def test_votes_plain(self):    self.assertEqual(transform_votes("500"), 500)
    def test_votes_none(self):     self.assertIsNone(transform_votes(None))
    def test_votes_nan(self):      self.assertIsNone(transform_votes(float("nan")))
    def test_votes_empty(self):    self.assertIsNone(transform_votes(""))

    # transform_year
    def test_year_valid(self):     self.assertEqual(transform_year("2015"), 2015)
    def test_year_old(self):       self.assertIsNone(transform_year("1800"))
    def test_year_future(self):    self.assertIsNone(transform_year("2099"))
    def test_year_none(self):      self.assertIsNone(transform_year(None))

    # transform_price
    def test_price_gbp(self):      self.assertEqual(transform_price("GBP 76.50"), 76.50)
    def test_price_aed(self):      self.assertEqual(transform_price("AED 345.00"), 345.0)
    def test_price_pound(self):    self.assertEqual(transform_price("\u00a389.99"), 89.99)
    def test_price_none(self):     self.assertIsNone(transform_price(None))
    def test_price_garbage(self):  self.assertIsNone(transform_price("no price"))

    # transform_bottle_size
    def test_bottle_ml(self):      self.assertEqual(transform_bottle_size("Sauvage 100ml"), 100.0)
    def test_bottle_oz(self):      self.assertAlmostEqual(transform_bottle_size("3.4 fl oz"), 100.5, places=0)
    def test_bottle_default(self): self.assertEqual(transform_bottle_size("no size"), 100.0)
    def test_bottle_none(self):    self.assertEqual(transform_bottle_size(None), 100.0)

    # convert_to_eur
    def test_eur_gbp(self):        self.assertEqual(convert_to_eur(100, "GBP"), 117.0)
    def test_eur_aed(self):        self.assertEqual(convert_to_eur(400, "AED"), 100.0)
    def test_eur_identity(self):   self.assertEqual(convert_to_eur(50, "EUR"), 50.0)
    def test_eur_unknown(self):    self.assertIsNone(convert_to_eur(100, "XYZ"))
    def test_eur_none(self):       self.assertIsNone(convert_to_eur(None, "GBP"))

    # compute_price_per_ml
    def test_ppm_normal(self):     self.assertEqual(compute_price_per_ml(100.0, 100.0), 1.0)
    def test_ppm_zero(self):       self.assertIsNone(compute_price_per_ml(100.0, 0))
    def test_ppm_none(self):       self.assertIsNone(compute_price_per_ml(None, 100.0))

    # classify_price_band
    def test_band_budget(self):    self.assertEqual(classify_price_band(0.15), "budget")
    def test_band_affordable(self): self.assertEqual(classify_price_band(0.45), "affordable")
    def test_band_luxury(self):    self.assertEqual(classify_price_band(3.50), "luxury")
    def test_band_ultra(self):     self.assertEqual(classify_price_band(7.00), "ultra_luxury")
    def test_band_none(self):      self.assertEqual(classify_price_band(None), "unknown")

    # transform_amazon_rating
    def test_amz_rating(self):     self.assertEqual(transform_amazon_rating("4.5 out of 5 stars"), 4.5)
    def test_amz_rating_none(self): self.assertIsNone(transform_amazon_rating(None))


class TestIntegration(unittest.TestCase):
    """Integration test: raw data -> transform -> DB -> query."""

    def test_full_pipeline(self):
        # Simulate raw scraped data
        raw_frag = pd.DataFrame([{
            "scrape_date": "2025-01-01", "name": "Test Perfume", "brand": "TestBrand",
            "category": "designer", "fragrantica_url": "https://test.com",
            "year_raw": "2020", "rating_raw": "4.25", "votes_raw": "1,500",
            "gender_raw": "male", "top_notes_raw": "Bergamot, Pepper",
            "middle_notes_raw": "Rose, Jasmine", "base_notes_raw": "Vanilla, Musk",
            "season_spring_raw": "60", "season_summer_raw": "45",
            "season_fall_raw": "70", "season_winter_raw": "80"}])

        raw_amz = pd.DataFrame([{
            "scrape_date": "2025-01-01", "name": "Test Perfume", "brand": "TestBrand",
            "category": "designer", "country": "UK", "currency": "GBP",
            "price_raw": "GBP 85.00", "bottle_size_raw": "Test 100ml",
            "amazon_rating_raw": "4.5 out of 5 stars", "amazon_reviews_raw": "2,300",
            "availability": "in_stock", "amazon_url": "https://amazon.co.uk/test"}])

        # Transform
        fc = clean_fragrantica(raw_frag)
        ac = clean_amazon(raw_amz)

        # Verify transformations
        self.assertEqual(fc["rating"].iloc[0], 4.25)
        self.assertEqual(fc["votes"].iloc[0], 1500)
        self.assertEqual(fc["year"].iloc[0], 2020)
        self.assertEqual(fc["top_notes"].iloc[0], "Bergamot, Pepper")
        self.assertEqual(ac["price"].iloc[0], 85.0)
        self.assertEqual(ac["price_eur"].iloc[0], 99.45)

        # Load to DB
        db = "test_integration.db"
        if os.path.exists(db):
            os.remove(db)
        summary = load_to_database(fc, ac, path=db)

        self.assertEqual(summary["brands"], 1)
        self.assertEqual(summary["fragrances"], 1)
        self.assertEqual(summary["fragrantica_data"], 1)
        self.assertEqual(summary["amazon_prices"], 1)

        # Query and verify
        conn = sqlite3.connect(db)
        c = conn.cursor()
        c.execute("SELECT rating, top_notes FROM fragrantica_data")
        row = c.fetchone()
        self.assertEqual(row[0], 4.25)
        self.assertEqual(row[1], "Bergamot, Pepper")

        c.execute("SELECT price_eur, country FROM amazon_prices")
        row = c.fetchone()
        self.assertEqual(row[0], 99.45)
        self.assertEqual(row[1], "UK")

        conn.close()
        os.remove(db)


if __name__ == "__main__":
    unittest.main(verbosity=2)
