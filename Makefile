# GitHub Activity Lakehouse — Makefile
# Run `make help` for a summary of available targets.

.PHONY: help install test lint download pipeline stream backfill rebuild stats clean

# Prefer the project venv (has pyspark/delta) over whatever python3 is on PATH.
PYTHON ?= $(if $(wildcard .venv/bin/python),.venv/bin/python,python3)

# Spark 3.5 supports Java 8/11/17.  On macOS pick a JDK 17 if one is installed
# and JAVA_HOME isn't already set.
ifeq ($(JAVA_HOME),)
JAVA_HOME := $(shell /usr/libexec/java_home -v 17 2>/dev/null)
endif
ifneq ($(JAVA_HOME),)
export JAVA_HOME
endif

help:
	@echo ""
	@echo "GitHub Activity Lakehouse — available targets"
	@echo "---------------------------------------------"
	@echo "  make install    Install Python dependencies"
	@echo "  make test       Run the full test suite (pytest)"
	@echo "  make lint       Run flake8 style checks"
	@echo "  make download   Download 3 days of GH Archive data (2024-01-01 to 2024-01-03)"
	@echo "  make pipeline   Incremental streaming run (availableNow): only new data, then exit"
	@echo "  make stream     Long-running streaming pipeline (1-minute micro-batches)"
	@echo "  make backfill   Chunked download + stream of BACKFILL_START..BACKFILL_END (~125M events)"
	@echo "  make rebuild    Batch full rebuild of every layer (idempotent)"
	@echo "  make stats      Print row counts / Delta versions of every table"
	@echo "  make clean      Remove generated data/ directories"
	@echo ""

# ---------------------------------------------------------------------------
# install
# ---------------------------------------------------------------------------
install:
	test -d .venv || python3 -m venv .venv
	.venv/bin/pip install -r requirements.txt

# ---------------------------------------------------------------------------
# test
# ---------------------------------------------------------------------------
test:
	PYTHONPATH=. $(PYTHON) -m pytest tests/ -v --tb=short

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
# Jan 1-28 2024 (four full weeks) is ~125M events.
BACKFILL_START ?= 2024-01-01
BACKFILL_END   ?= 2024-01-28

pipeline:
	PYTHONPATH=. $(PYTHON) scripts/run_pipeline.py --streaming \
		--bronze --silver --gold --dq --stats

stream:
	PYTHONPATH=. $(PYTHON) scripts/run_pipeline.py --streaming \
		--bronze --silver --gold --trigger-interval "1 minute"

# Downloads and streams a few days at a time, deleting raw files once
# ingested, so peak disk use is the lakehouse plus one chunk of raw data.
backfill:
	PYTHONPATH=. $(PYTHON) scripts/backfill.py \
		--start $(BACKFILL_START) --end $(BACKFILL_END)

rebuild:
	PYTHONPATH=. $(PYTHON) scripts/run_pipeline.py \
		--bronze --silver --dq --gold --stats

stats:
	PYTHONPATH=. $(PYTHON) scripts/run_pipeline.py --stats

# ---------------------------------------------------------------------------
# clean
# ---------------------------------------------------------------------------
clean:
	rm -rf data/bronze data/silver data/gold data/quarantine data/checkpoints
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true
