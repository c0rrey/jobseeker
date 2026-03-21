"""Tests for pipeline/config/settings.py.

Covers the active getter functions:
- get_rapidapi_key()
- get_db_path()

Also verifies that the existing get_adzuna_credentials() function is unchanged
and that python-dotenv loading does not break the module import.
"""

import os
from pathlib import Path

import pytest

from pipeline.config.settings import (
    PROJECT_ROOT,
    get_adzuna_credentials,
    get_db_path,
    get_rapidapi_key,
)


# ---------------------------------------------------------------------------
# get_rapidapi_key
# ---------------------------------------------------------------------------


class TestGetRapidapiKey:
    """Tests for get_rapidapi_key()."""

    def test_returns_value_when_set(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Returns the env var value when RAPIDAPI_KEY is set."""
        monkeypatch.setenv("RAPIDAPI_KEY", "test-rapid-key")
        assert get_rapidapi_key() == "test-rapid-key"

    def test_raises_value_error_when_missing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Raises ValueError with a descriptive message when RAPIDAPI_KEY is absent."""
        monkeypatch.delenv("RAPIDAPI_KEY", raising=False)
        with pytest.raises(ValueError, match="RAPIDAPI_KEY"):
            get_rapidapi_key()

    def test_raises_value_error_when_empty_string(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Raises ValueError when RAPIDAPI_KEY is set to an empty string."""
        monkeypatch.setenv("RAPIDAPI_KEY", "")
        with pytest.raises(ValueError, match="RAPIDAPI_KEY"):
            get_rapidapi_key()

    def test_error_message_mentions_rapidapi(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The ValueError message references RapidAPI to aid diagnosis."""
        monkeypatch.delenv("RAPIDAPI_KEY", raising=False)
        with pytest.raises(ValueError, match="(?i)rapidapi"):
            get_rapidapi_key()


# ---------------------------------------------------------------------------
# get_db_path
# ---------------------------------------------------------------------------


class TestGetDbPath:
    """Tests for get_db_path()."""

    def test_returns_custom_path_when_set(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Returns the DB_PATH value when the env var is set."""
        monkeypatch.setenv("DB_PATH", "/tmp/custom.db")
        assert get_db_path() == "/tmp/custom.db"

    def test_returns_default_when_not_set(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Returns a default path ending in data/jobs.db when DB_PATH is unset."""
        monkeypatch.delenv("DB_PATH", raising=False)
        result = get_db_path()
        assert result.endswith("data/jobs.db"), f"Unexpected default: {result}"

    def test_default_path_is_under_project_root(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The default path is rooted at the project root, not a temp dir."""
        monkeypatch.delenv("DB_PATH", raising=False)
        result = get_db_path()
        assert result.startswith(str(PROJECT_ROOT)), (
            f"Default DB path {result!r} is not under project root {PROJECT_ROOT}"
        )

    def test_does_not_raise_when_missing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """get_db_path() never raises — missing DB_PATH uses the default."""
        monkeypatch.delenv("DB_PATH", raising=False)
        # Should not raise
        path = get_db_path()
        assert isinstance(path, str)


# ---------------------------------------------------------------------------
# get_adzuna_credentials (regression — must be unchanged)
# ---------------------------------------------------------------------------


class TestGetAdzunaCredentials:
    """Regression tests ensuring get_adzuna_credentials() is unchanged."""

    def test_returns_tuple_when_both_set(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Returns (app_id, app_key) tuple when both env vars are set."""
        monkeypatch.setenv("ADZUNA_APP_ID", "id-123")
        monkeypatch.setenv("ADZUNA_APP_KEY", "key-abc")
        result = get_adzuna_credentials()
        assert result == ("id-123", "key-abc")

    def test_raises_when_app_id_missing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Raises ValueError when ADZUNA_APP_ID is absent."""
        monkeypatch.delenv("ADZUNA_APP_ID", raising=False)
        monkeypatch.setenv("ADZUNA_APP_KEY", "key-abc")
        with pytest.raises(ValueError, match="ADZUNA_APP_ID"):
            get_adzuna_credentials()

    def test_raises_when_app_key_missing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Raises ValueError when ADZUNA_APP_KEY is absent."""
        monkeypatch.setenv("ADZUNA_APP_ID", "id-123")
        monkeypatch.delenv("ADZUNA_APP_KEY", raising=False)
        with pytest.raises(ValueError, match="ADZUNA_APP_KEY"):
            get_adzuna_credentials()

    def test_raises_when_both_missing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Raises ValueError when both Adzuna credentials are absent."""
        monkeypatch.delenv("ADZUNA_APP_ID", raising=False)
        monkeypatch.delenv("ADZUNA_APP_KEY", raising=False)
        with pytest.raises(ValueError):
            get_adzuna_credentials()


# ---------------------------------------------------------------------------
# Module-level dotenv loading
# ---------------------------------------------------------------------------


class TestDotenvLoading:
    """Verify that python-dotenv integration does not break module behaviour."""

    def test_module_imports_without_env_file(self) -> None:
        """settings.py imports cleanly even when no .env file exists."""
        # If we reach here the module already imported successfully.
        from pipeline.config import settings  # noqa: F401

        assert hasattr(settings, "PROJECT_ROOT")
        assert hasattr(settings, "get_rapidapi_key")
        assert hasattr(settings, "get_db_path")

    def test_project_root_resolves_correctly(self) -> None:
        """PROJECT_ROOT points to the jseeker repo root."""
        # pipeline/config/settings.py -> pipeline/config -> pipeline -> jseeker
        expected = Path(__file__).resolve().parent.parent.parent
        assert PROJECT_ROOT == expected
