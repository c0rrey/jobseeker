"""
Job fetcher package.

Provides BaseFetcher and concrete fetcher implementations.
All fetchers return raw dicts that the normalizer converts to Job dataclasses.
"""

from .adzuna import AdzunaFetcher
from .base import BaseFetcher
from .remoteok import RemoteOKFetcher

__all__ = ["BaseFetcher", "AdzunaFetcher", "RemoteOKFetcher"]
