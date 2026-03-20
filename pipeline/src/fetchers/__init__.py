"""
Job fetcher package.

Provides BaseFetcher and concrete fetcher implementations.
All fetchers return raw dicts that the normalizer converts to Job dataclasses.
"""

from .adzuna import AdzunaFetcher
from .ats import ATSFetcher
from .base import BaseFetcher
from .career_page import CareerPageFetcher
from .linkedin import LinkedInFetcher
from .remoteok import RemoteOKFetcher

__all__ = [
    "BaseFetcher",
    "AdzunaFetcher",
    "ATSFetcher",
    "CareerPageFetcher",
    "LinkedInFetcher",
    "RemoteOKFetcher",
]
