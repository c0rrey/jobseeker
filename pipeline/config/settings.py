"""
Load and validate configuration from YAML files and environment variables.

Provides a single place to access profile, red flags, and other settings.
"""

import os
from pathlib import Path

import yaml


# Path to the config directory (same folder as this file)
CONFIG_DIR = Path(__file__).resolve().parent


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
        ValueError: If credentials are not set
    """
    app_id = os.getenv("ADZUNA_APP_ID")
    app_key = os.getenv("ADZUNA_APP_KEY")
    
    if not app_id or not app_key:
        raise ValueError(
            "Adzuna credentials not found. "
            "Set ADZUNA_APP_ID and ADZUNA_APP_KEY environment variables."
        )
    
    return app_id, app_key
