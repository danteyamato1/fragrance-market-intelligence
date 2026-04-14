PYTHON    = python
LIMIT    ?= 5
DB        = fragrance_market.db
CLEAN_DIR = cleaned_data

.PHONY: help install install-playwright \
        scrape scrape-fragrantica scrape-amazon \
        scrape-test scrape-amazon-test scrape-all-test smoke \
        features analyze \
        test clean clean-raw clean-db

help:
	@echo ""
	@echo "  Fragrance Market Intelligence"
	@echo "  make install              Install Python dependencies"
	@echo "  make install-playwright   Install Playwright + Chromium"
	@echo ""
	@echo "  make scrape               Full pipeline (both scrapers + features + DB + analysis)"
	@echo "  make scrape-fragrantica   Fragrantica only"
	@echo "  make scrape-amazon        Amazon USA + UAE only"
	@echo ""
	@echo "  make scrape-test          Quick run — Fragrantica, LIMIT=$(LIMIT)"
	@echo "  make scrape-amazon-test   Quick run — Amazon,      LIMIT=$(LIMIT)"
	@echo "  make scrape-all-test      Quick run — BOTH (full pipeline), LIMIT=$(LIMIT)"
	@echo "  make smoke                Smallest end-to-end sanity check, LIMIT=3"
	@echo ""
	@echo "  make features             Re-run feature engineering + DB dump (no scrape)"
	@echo "  make analyze              Re-run the 4 analyses from existing cleaned CSVs"
	@echo ""
	@echo "  make test                 Run unit + integration tests"
	@echo "  make clean                Remove all generated data"
	@echo "  make clean-raw            Remove raw CSVs only"
	@echo "  make clean-db             Remove SQLite database only"
	@echo ""
	@echo "  Override limit: make scrape-all-test LIMIT=10"
	@echo ""


install:
	$(PYTHON) -m pip install -r requirements.txt

install-playwright:
	$(PYTHON) -m pip install playwright playwright-stealth
	$(PYTHON) -m playwright install chromium

scrape:
	$(PYTHON) daily_scraper.py

scrape-fragrantica:
	$(PYTHON) daily_scraper.py --fragrantica-only

scrape-amazon:
	$(PYTHON) daily_scraper.py --amazon-only


scrape-test:
	$(PYTHON) daily_scraper.py --fragrantica-only --limit $(LIMIT)

scrape-amazon-test:
	$(PYTHON) daily_scraper.py --amazon-only --limit $(LIMIT)

scrape-all-test:
	$(PYTHON) daily_scraper.py --limit $(LIMIT)

smoke:
	$(PYTHON) daily_scraper.py --limit 3


features:
	$(PYTHON) -c "import pandas as pd, datetime as dt; \
	from feature_engineering import engineer_features; \
	from database import create_database; \
	from database_features import load_features; \
	merged = pd.read_csv('$(CLEAN_DIR)/merged_normalized.csv'); \
	feats, _ = engineer_features(merged); \
	conn = create_database(); \
	n = load_features(conn, feats, dt.date.today().isoformat()); \
	conn.close(); \
	print(f'[features] wrote {n} rows to $(DB)')"

analyze:
	$(PYTHON) -c "import pandas as pd; \
	from feature_engineering import engineer_features; \
	from analysis import run_all; \
	merged = pd.read_csv('$(CLEAN_DIR)/merged_normalized.csv'); \
	feats, _ = engineer_features(merged); \
	run_all(feats); \
	print('[analyze] done — see $(CLEAN_DIR)/analysis_*.csv')"

report:
	$(PYTHON) report.py


test:
	$(PYTHON) daily_scraper.py --test

clean: clean-raw clean-db
	@echo "All generated data removed."

clean-raw:
	@$(PYTHON) -c "import shutil; [shutil.rmtree(d, ignore_errors=True) for d in ['raw_data','cleaned_data']]"
	@echo "Raw and cleaned data removed."

clean-db:
	@$(PYTHON) -c "import os; [os.remove(f) for f in ['$(DB)','fragrance.db'] if os.path.exists(f)]"
	@echo "Database removed."
