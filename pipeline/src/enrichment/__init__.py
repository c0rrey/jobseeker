"""
Enrichment package for company metadata.

Provides two independent enrichment modules that each query an external
source and write results to the companies table in the V2 database.

Exported functions:
    glassdoor_rapidapi.enrich  -- glassdoor_rating and sub-ratings via RapidAPI
    levelsfy.enrich            -- compensation data (stored in crunchbase_data)
"""

from pipeline.src.enrichment.glassdoor_rapidapi import enrich as enrich_glassdoor
from pipeline.src.enrichment.levelsfy import enrich as enrich_levelsfy

__all__ = [
    "enrich_glassdoor",
    "enrich_levelsfy",
]
