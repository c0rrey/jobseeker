PYTHON  ?= python3
DB_PATH ?= data/jobs.db

.PHONY: setup fetch enrich prefilter all web db-reset test

## Bootstrap the project: install Python + Node dependencies.
setup:
	$(PYTHON) -m pip install -r pipeline/requirements.txt
	cd web && npm install

## Run all API fetchers, ATS feeds, and career page crawler.
fetch:
	$(PYTHON) -m pipeline.cli --fetch --db $(DB_PATH)

## Run the enrichment orchestrator on companies needing enrichment.
enrich:
	$(PYTHON) -m pipeline.cli --enrich --db $(DB_PATH)

## Run the deterministic pre-filter on unfiltered jobs.
prefilter:
	$(PYTHON) -m pipeline.cli --prefilter --db $(DB_PATH)

## Run fetch, prefilter, and enrich in sequence.
all:
	$(PYTHON) -m pipeline.cli --all --db $(DB_PATH)

## Start the Next.js dev server.
web:
	cd web && npm run dev

## Delete and recreate the database with the V2 schema.
db-reset:
	rm -f $(DB_PATH)
	$(PYTHON) -c "from pipeline.src.database import init_db; init_db('$(DB_PATH)')"

## Run the Python test suite.
test:
	$(PYTHON) -m pytest pipeline/tests/ -q
