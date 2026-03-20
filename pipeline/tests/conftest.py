"""Pytest configuration for pipeline tests."""

import sys
from pathlib import Path

# Add the pipeline directory to sys.path so that config.settings is importable
# without installing the package.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
