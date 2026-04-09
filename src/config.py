"""Project-wide filesystem constants."""

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
NOTEBOOKS_DIR = PROJECT_ROOT / "notebooks"
MODELS_DIR = PROJECT_ROOT / "models"
REPORTS_DIR = PROJECT_ROOT / "reports"
FIGURES_DIR = REPORTS_DIR / "figures"
LISTINGS_PATH = RAW_DATA_DIR / "listings.csv"

__all__ = [
    "PROJECT_ROOT",
    "DATA_DIR",
    "RAW_DATA_DIR",
    "PROCESSED_DATA_DIR",
    "NOTEBOOKS_DIR",
    "MODELS_DIR",
    "REPORTS_DIR",
    "FIGURES_DIR",
    "LISTINGS_PATH",
]
