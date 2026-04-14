"""
Unit tests for feature_engineering.py
Append these to tests.py, or run standalone with: python -m unittest
"""

import unittest
import pandas as pd
import numpy as np

from feature_engineering import (
    engineer_features,
    _classify_origin,
    _sentiment_polarisation,
    _crowd_pleaser,
    _season_dominant,
    _note_count,
)


def _sample_df():
    return pd.DataFrame([
        {
            "brand": "Dior", "name": "Sauvage", "gender": "male",
            "year": 2015, "rating": 4.2, "votes": 21000,
            "price_per_ml_eur": 1.5, "price_band": "premium",
            "fragrance_family": "Aromatic Fougere",
            "votes_love": 500, "votes_like": 300, "votes_ok": 100,
            "votes_dislike": 50, "votes_hate": 50,
            "season_spring": 40, "season_summer": 80,
            "season_fall": 60, "season_winter": 30,
            "top_notes": "bergamot, pepper", "middle_notes": "lavender",
            "base_notes": "ambroxan, cedar",
            "price_eur_usa": 120, "price_eur_uae": 85,
        },
        {
            "brand": "Lattafa", "name": "Asad", "gender": "male",
            "year": 2020, "rating": 4.0, "votes": 3000,
            "price_per_ml_eur": 0.2, "price_band": "budget",
            "fragrance_family": "Amber Woody",
            "votes_love": 200, "votes_like": 400, "votes_ok": 150,
            "votes_dislike": 80, "votes_hate": 170,
            "season_spring": 10, "season_summer": 15,
            "season_fall": 70, "season_winter": 90,
            "top_notes": "saffron", "middle_notes": "rose, oud",
            "base_notes": "amber",
            "price_eur_usa": 35, "price_eur_uae": 20,
        },
    ])


class TestHelpers(unittest.TestCase):

    def test_classify_origin(self):
        self.assertEqual(_classify_origin("Lattafa"), "arabic")
        self.assertEqual(_classify_origin("Dior"), "designer")
        self.assertEqual(_classify_origin("Creed"), "niche")
        self.assertEqual(_classify_origin(None), "unknown")

    def test_sentiment_polarisation_bounds(self):
        row = {"votes_love": 100, "votes_like": 0, "votes_ok": 0,
               "votes_dislike": 0, "votes_hate": 0}
        self.assertEqual(_sentiment_polarisation(row), 1.0)
        row["votes_hate"] = 100
        row["votes_love"] = 0
        self.assertEqual(_sentiment_polarisation(row), -1.0)

    def test_sentiment_polarisation_zero_votes(self):
        row = {k: 0 for k in ("votes_love", "votes_like", "votes_ok",
                              "votes_dislike", "votes_hate")}
        self.assertTrue(np.isnan(_sentiment_polarisation(row)))

    def test_crowd_pleaser_range(self):
        row = {"votes_love": 50, "votes_like": 50, "votes_ok": 0,
               "votes_dislike": 0, "votes_hate": 0}
        self.assertEqual(_crowd_pleaser(row), 1.0)

    def test_season_dominant(self):
        row = {"season_spring": 5, "season_summer": 80,
               "season_fall": 10, "season_winter": 5}
        self.assertEqual(_season_dominant(row), "summer")

    def test_note_count_deduplicates(self):
        row = {"top_notes": "rose, oud",
               "middle_notes": "rose",
               "base_notes": "amber"}
        self.assertEqual(_note_count(row), 3)


class TestEngineerFeatures(unittest.TestCase):

    def setUp(self):
        self.df = _sample_df()
        self.out, self.enc = engineer_features(self.df)

    def test_returns_dataframe_and_dict(self):
        self.assertIsInstance(self.out, pd.DataFrame)
        self.assertIsInstance(self.enc, dict)
        self.assertEqual(len(self.out), 2)

    def test_brand_origin_added(self):
        self.assertIn("brand_origin", self.out.columns)
        self.assertEqual(set(self.out["brand_origin"]), {"designer", "arabic"})

    def test_label_encoders_present(self):
        self.assertIn("label_gender", self.enc)
        self.assertIn("label_brand_origin", self.enc)
        self.assertIn("gender_id", self.out.columns)
        self.assertIn("brand_origin_id", self.out.columns)

    def test_one_hot_columns_created(self):
        fam_cols = [c for c in self.out.columns if c.startswith("fam_")]
        band_cols = [c for c in self.out.columns if c.startswith("band_")]
        self.assertGreater(len(fam_cols), 0)
        self.assertGreater(len(band_cols), 0)
        for cols in (fam_cols, band_cols):
            if cols:
                self.assertTrue((self.out[cols].sum(axis=1) == 1).all())

    def test_scaled_columns_in_unit_interval(self):
        for c in ("rating_scaled", "price_per_ml_eur_scaled", "votes_scaled"):
            if c in self.out.columns:
                self.assertTrue(((self.out[c] >= 0) & (self.out[c] <= 1)).all())

    def test_value_score_present(self):
        self.assertIn("value_score", self.out.columns)

    def test_usa_uae_gap_sign(self):
        # Both rows have USA > UAE, so gap should be positive
        self.assertTrue((self.out["usa_uae_gap_pct"] > 0).all())

    def test_years_on_market_nonnegative(self):
        self.assertTrue((self.out["years_on_market"] >= 0).all())

    def test_empty_input_returns_empty(self):
        out, enc = engineer_features(pd.DataFrame())
        self.assertTrue(out.empty)
        self.assertEqual(enc, {})


if __name__ == "__main__":
    unittest.main()
