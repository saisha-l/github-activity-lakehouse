# GitHub Activity Lakehouse — Makefile
# Run `make help` for a summary of available targets.

.PHONY: help install test lint download pipeline clean

PYTHON ?= python3

help:
	@echo ""
	@echo "GitHub Activity Lakehouse — available targets"
	@echo "---------------------------------------------"
	@echo "  make install    Install Python dependencies"
	@echo "  make test       Run the full test suite (pytest)"
	@echo "  make lint       Run flake8 style checks"
	@echo "  make download   Download 3 days of GH Archive data (2024-01-01 to 2024-01-03)"
	@echo "  make pipeline   Run the full pipeline (Bronze → Silver → Gold)"
	@echo "  make clean      Remove generated data/ directories"
	@echo ""

# ---------------------------------------------------------------------------
# install
# ---------------------------------------------------------------------------
install:
	pip install -r requirements.txt

# ---------------------------------------------------------------------------
# test
# ---------------------------------------------------------------------------
test:
	PYTHONPATH=. pytest tests/ -v --tb=short

# ---------------------------------------------------------------------------
# lint
# ---------------------------------------------------------------------------
lint:
	@command -v flake8 >/dev/null 2>&1 || pip install flake8 -q
	flake8 src/ conf/ scripts/ tests/ \
		--max-line-length=100 \
		--extend-ignore=E203,W503,E501 \
		--exclude=__pycache__,.git,.venv

# ---------------------------------------------------------------------------
# download
# ---------------------------------------------------------------------------
download:
	PYTHONPATH=. $(PYTHON) scripts/download_data.py \
		--start 2024-01-01 \
		--end 2024-01-03

# ---------------------------------------------------------------------------
# pipeline
# ---------------------------------------------------------------------------
pipeline:
	PYTHONPATH=. $(PYTHON) scripts/run_pipeline.py \
		--bronze --silver --dq --gold

# ---------------------------------------------------------------------------
# clean
# ---------------------------------------------------------------------------
clean:
	rm -rf data/bronze data/silver data/gold data/quarantine data/checkpoints
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true
