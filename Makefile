# ============================================================
#  Fragrance Market Intelligence — Makefile
#  Usage: make <target> [LIMIT=5]
# ============================================================

PYTHON  = python
LIMIT   ?= 5

.PHONY: help install install-playwright \
        scrape scrape-fragrantica scrape-amazon \
        scrape-test scrape-amazon-test scrape-all-test \
        test clean clean-raw clean-db

help:
	@echo ""
	@echo "  Fragrance Market Intelligence"
	@echo "  --------------------------------"
	@echo "  make install              Install Python dependencies"
	@echo "  make install-playwright   Install Playwright + Chromium"
	@echo ""
	@echo "  make scrape               Full pipeline (Fragrantica + Amazon)"
	@echo "  make scrape-fragrantica   Fragrantica only"
	@echo "  make scrape-amazon        Amazon USA + UAE only (all products)"
	@echo ""
	@echo "  make scrape-test          Test run — Fragrantica, limit $(LIMIT)"
	@echo "  make scrape-amazon-test   Test run — Amazon, limit $(LIMIT)"
	@echo "  make scrape-all-test      Test run — both scrapers, limit $(LIMIT)"
	@echo ""
	@echo "  make test                 Run unit + integration tests"
	@echo "  make clean                Remove all generated data"
	@echo "  make clean-raw            Remove raw CSVs only"
	@echo "  make clean-db             Remove SQLite database only"
	@echo ""
	@echo "  Override limit: make scrape-amazon-test LIMIT=10"
	@echo ""

# ── Installation ─────────────────────────────────────────────

install:
	$(PYTHON) -m pip install -r requirements.txt

install-playwright:
	$(PYTHON) -m pip install playwright playwright-stealth
	$(PYTHON) -m playwright install chromium

# ── Full pipeline ─────────────────────────────────────────────

scrape:
	$(PYTHON) daily_scraper.py

scrape-fragrantica:
	$(PYTHON) daily_scraper.py --fragrantica-only

scrape-amazon:
	$(PYTHON) daily_scraper.py --amazon-only

# ── Test runs (limited products) ─────────────────────────────

scrape-test:
	$(PYTHON) daily_scraper.py --fragrantica-only --limit $(LIMIT)

scrape-amazon-test:
	$(PYTHON) daily_scraper.py --amazon-only --limit $(LIMIT)

scrape-all-test:
	$(PYTHON) daily_scraper.py --limit $(LIMIT)

# ── Unit tests ───────────────────────────────────────────────

test:
	$(PYTHON) daily_scraper.py --test

# ── Cleanup ──────────────────────────────────────────────────

clean: clean-raw clean-db
	@echo "All generated data removed."

clean-raw:
	@$(PYTHON) -c "import shutil, os; [shutil.rmtree(d, ignore_errors=True) for d in ['raw_data','cleaned_data']]"
	@echo "Raw and cleaned data removed."

clean-db:
	@$(PYTHON) -c "import os; [os.remove(f) for f in ['fragrance.db'] if os.path.exists(f)]"
	@echo "Database removed."
