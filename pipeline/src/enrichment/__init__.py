"""
Enrichment package for company metadata.

Provides four independent enrichment modules that each query an external
source and write results to the companies table in the V2 database.

Exported functions:
    crunchbase.enrich  -- company size, funding stage, industry
    glassdoor.enrich   -- glassdoor_rating, glassdoor_url
    levelsfy.enrich    -- compensation data (stored in crunchbase_data)
    stackshare.enrich  -- tech_stack
"""

from pipeline.src.enrichment.crunchbase import enrich as enrich_crunchbase
from pipeline.src.enrichment.glassdoor import enrich as enrich_glassdoor
from pipeline.src.enrichment.levelsfy import enrich as enrich_levelsfy
from pipeline.src.enrichment.stackshare import enrich as enrich_stackshare

__all__ = [
    "enrich_crunchbase",
    "enrich_glassdoor",
    "enrich_levelsfy",
    "enrich_stackshare",
]
