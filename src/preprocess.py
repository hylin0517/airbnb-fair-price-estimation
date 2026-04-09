"""Preprocessing utilities for audit and EDA-ready listings data."""

import re
from collections.abc import Iterable

import pandas as pd


DEFAULT_DATE_COLUMNS = (
    "last_review",
    "first_review",
    "host_since",
    "calendar_last_scraped",
    "last_scraped",
)


def standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy with lowercase snake_case column names."""
    standardized = df.copy()
    standardized.columns = [_normalize_column_name(column) for column in standardized.columns]
    return standardized


def clean_price_column(price: pd.Series) -> pd.Series:
    """Convert a price column to numeric, handling currency symbols and commas."""
    if pd.api.types.is_numeric_dtype(price):
        return pd.to_numeric(price, errors="coerce")

    cleaned = (
        price.astype("string")
        .str.strip()
        .str.replace(r"[\$,]", "", regex=True)
        .str.replace(r"[^\d\.\-]", "", regex=True)
    )
    cleaned = cleaned.replace({"": pd.NA, "-": pd.NA, ".": pd.NA, "-.": pd.NA})
    return pd.to_numeric(cleaned, errors="coerce")


def coerce_obvious_dtypes(
    df: pd.DataFrame,
    *,
    date_columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    """Return a copy with conservative type fixes applied to obvious date columns."""
    coerced = df.copy()

    columns_to_parse = tuple(date_columns) if date_columns is not None else DEFAULT_DATE_COLUMNS

    for column in columns_to_parse:
        if column in coerced.columns:
            coerced[column] = pd.to_datetime(coerced[column], errors="coerce")

    return coerced


def prepare_listings_for_eda(df: pd.DataFrame) -> pd.DataFrame:
    """Return a cleaned listings dataframe suitable for data audit and EDA."""
    cleaned = standardize_column_names(df)

    if "price" not in cleaned.columns:
        raise ValueError("Expected a 'price' column after column standardization.")

    cleaned["price"] = clean_price_column(cleaned["price"])
    cleaned = coerce_obvious_dtypes(cleaned)

    return cleaned


def _normalize_column_name(name: object) -> str:
    """Normalize a column name to lowercase snake_case."""
    normalized = str(name).strip().lower()
    normalized = re.sub(r"[^\w]+", "_", normalized)
    normalized = re.sub(r"_+", "_", normalized)
    return normalized.strip("_")


__all__ = [
    "DEFAULT_DATE_COLUMNS",
    "clean_price_column",
    "coerce_obvious_dtypes",
    "prepare_listings_for_eda",
    "standardize_column_names",
]
