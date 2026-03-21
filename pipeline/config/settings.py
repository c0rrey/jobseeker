"""
Load and validate configuration from YAML files and environment variables.

Provides a single place to access profile, red flags, and other settings.
Environment variables are loaded from a .env file at the project root on
module import. Variables already present in the environment take precedence
over values in .env (override=False, 12-factor app convention).
"""

import os
from pathlib import Path

import yaml
from dotenv import load_dotenv


# Path to the config directory (same folder as this file)
CONFIG_DIR = Path(__file__).resolve().parent

# Project root is two levels up from this file (pipeline/config/settings.py)
PROJECT_ROOT = CONFIG_DIR.parent.parent

# Load .env from project root on module import. override=False means
# variables already set in the real environment win over .env values.
load_dotenv(PROJECT_ROOT / ".env", override=False)


def load_profile() -> dict:
    """Load and return the job search profile from profile.yaml."""
    path = CONFIG_DIR / "profile.yaml"
    with path.open() as f:
        return yaml.safe_load(f)


def load_red_flags() -> dict:
    """Load and return red-flag rules from red_flags.yaml."""
    path = CONFIG_DIR / "red_flags.yaml"
    with path.open() as f:
        return yaml.safe_load(f)


def get_adzuna_credentials() -> tuple[str, str]:
    """
    Get Adzuna API credentials from environment variables.

    Returns:
        Tuple of (app_id, app_key)

    Raises:
        ValueError: If credentials are not set.
    """
    app_id = os.getenv("ADZUNA_APP_ID")
    app_key = os.getenv("ADZUNA_APP_KEY")

    if not app_id or not app_key:
        raise ValueError(
            "Adzuna credentials not found. "
            "Set ADZUNA_APP_ID and ADZUNA_APP_KEY environment variables."
        )

    return app_id, app_key


def get_rapidapi_key() -> str:
    """
    Get the RapidAPI key from environment variables.

    Used for LinkedIn job search via RapidAPI.

    Returns:
        The RAPIDAPI_KEY value.

    Raises:
        ValueError: If RAPIDAPI_KEY is not set.
    """
    key = os.getenv("RAPIDAPI_KEY")
    if not key:
        raise ValueError(
            "RapidAPI key not found. "
            "Set the RAPIDAPI_KEY environment variable."
        )
    return key


def get_db_path() -> str:
    """
    Get the database file path from environment variables.

    Returns:
        The DB_PATH value if set, otherwise the default ``data/jobs.db``
        relative to the project root.
    """
    return os.getenv("DB_PATH", str(PROJECT_ROOT / "data" / "jobs.db"))
