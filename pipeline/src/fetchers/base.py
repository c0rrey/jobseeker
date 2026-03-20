"""
Abstract base class for job fetchers.

All fetchers (Adzuna, mock, RSS, etc.) implement this interface.
The pipeline calls fetch() and passes the raw results to the normalizer.
"""

from abc import ABC, abstractmethod
from typing import Any


class BaseFetcher(ABC):
    """Interface that all job fetchers must implement."""

    @abstractmethod
    def fetch(self) -> list[dict[str, Any]]:
        """
        Fetch raw job listings from the source.

        Returns:
            List of dicts in the source's native format.
            Each dict will be passed to the normalizer for that source.
        """
        pass
