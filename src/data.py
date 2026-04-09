"""Utilities for loading raw Airbnb listings data."""

from pathlib import Path

import pandas as pd

from .config import LISTINGS_PATH, PROJECT_ROOT


def resolve_listings_path(path: str | Path | None = None) -> Path:
    """Resolve a listings dataset path relative to the project root."""
    if path is None:
        return LISTINGS_PATH

    candidate = Path(path).expanduser()
    if not candidate.is_absolute():
        candidate = PROJECT_ROOT / candidate

    return candidate.resolve()


def load_listings(
    path: str | Path | None = None,
    *,
    low_memory: bool = False,
) -> pd.DataFrame:
    """Load the raw Airbnb listings dataset from the configured or provided path."""
    dataset_path = resolve_listings_path(path)

    if not dataset_path.exists():
        raise FileNotFoundError(f"Listings dataset not found at: {dataset_path}")

    return pd.read_csv(dataset_path, low_memory=low_memory)


__all__ = ["load_listings", "resolve_listings_path"]
