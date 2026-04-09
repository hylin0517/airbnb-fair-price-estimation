# Airbnb Price Prediction

A portfolio-style data science project for predicting Airbnb listing prices from historical Inside Airbnb listings data.

This repository is set up as a clean foundation for iterative work. Reusable Python code will live in `src/`, and analysis notebooks will live in `notebooks/`.

## Project Structure

```text
.
├── README.md
├── environment.yml
├── data/
│   ├── raw/
│   │   └── listings.csv
│   └── processed/
├── models/
├── notebooks/
├── reports/
│   └── figures/
└── src/
    ├── __init__.py
    └── config.py
```

## Setup

Create the environment and launch Jupyter:

```bash
conda env create -f environment.yml
conda activate airbnb-reg
jupyter lab
```

## Workflow

- Raw data lives in `data/raw/`.
- Intermediate datasets can be written to `data/processed/`.
- Reusable project code lives in `src/`.
- Future notebooks will be added incrementally for audit, EDA, modeling, and reporting.
