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
make all        # Run all three stages in sequence
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
| **score** | Two-pass LLM scoring (fast filter + deep analysis) run via Claude Code subagents, not the CLI. |
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
