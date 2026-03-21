# jseeker

Job search pipeline and web dashboard. Fetches listings from multiple sources, enriches company data, scores matches against your profile, and presents everything in a Next.js UI.

## Prerequisites

- Python 3.10+
- Node.js 20+
- npm

## Setup

```bash
# 1. Clone and enter the project
git clone https://github.com/c0rrey/jobseeker-v2.git jseeker
cd jseeker

# 2. Copy and fill in your API keys
cp .env.example .env
# Edit .env with your credentials

# 3. Install dependencies
make setup

# 4. Create the database
make db-reset
```

## Usage

### Pipeline stages

```bash
make fetch      # Fetch jobs from all sources
make enrich     # Enrich company metadata
make prefilter  # Run deterministic pre-filter
make all        # Run fetch, prefilter, and enrich in sequence
```

### Standalone scripts

```bash
# Fetch full job descriptions for Pass 1 survivors
python3 -m pipeline.scripts.fetch_descriptions --db data/jseeker.db

# Discover companies from Pass 1 survivors (also available as --discover via CLI)
python3 -m pipeline.scripts.discover_companies --db data/jseeker.db

# Or use the CLI flag:
python3 pipeline/cli.py --discover
```

### Web dashboard

```bash
make web        # Start Next.js dev server at http://localhost:3000
```

### Other commands

```bash
make test       # Run the Python test suite
make db-reset   # Delete and recreate the database
```

## Pipeline stages

| Stage | What it does |
|-------|-------------|
| **fetch** | Pulls jobs from Adzuna, RemoteOK, LinkedIn (RapidAPI), ATS feeds (Greenhouse/Lever/Ashby), and career page crawlers. Deduplicates and inserts into SQLite. |
| **enrich** | Enriches company records with data from Crunchbase, Glassdoor, Levels.fyi, and StackShare. |
| **prefilter** | Applies deterministic filters (location, red flags, staleness) to remove obvious non-matches. |
| **fetch-descriptions** | Fetches full job descriptions for Pass 1 survivors. Rate-limited, idempotent. |
| **discover** | Finds companies from Pass 1 survivors not yet in the companies table and enriches them. |
| **score** | Two-pass LLM scoring (fast filter + deep analysis) run via Claude Code subagents, not the CLI. Salary thresholds are driven by `profile.yaml`. |
| **profile** | Profile evolution analysis and resume improvement suggestions, also via Claude Code. |

## Project structure

```
pipeline/           Python pipeline code
  config/           Settings, profile YAML, red flags
  prompts/          LLM prompt templates
  resume/           Resume PDF storage
  src/              Source modules (fetchers, enrichment, scoring, etc.)
  tests/            Test suite
web/                Next.js dashboard app
data/               SQLite database (created by make db-reset)
```
