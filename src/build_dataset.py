from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"

JAN_LISTINGS_PATH = DATA_DIR / "listings_2026_01.csv"
FEB_LISTINGS_PATH = DATA_DIR / "listings_2026_02.csv"
JAN_REVIEWS_PATH = DATA_DIR / "reviews_2026_01.csv"
FEB_REVIEWS_PATH = DATA_DIR / "reviews_2026_02.csv"
OUTPUT_PATH = DATA_DIR / "airbnb_2026_market.csv"
REVIEW_WINDOWS = (30, 90, 365)

LISTING_FEATURES = [
    "price",
    "room_type",
    "neighbourhood",
    "neighbourhood_group",
    "latitude",
    "longitude",
    "minimum_nights",
    "number_of_reviews",
    "reviews_per_month",
    "calculated_host_listings_count",
    "availability_365",
    "number_of_reviews_ltm",
    "license",
]

LISTING_NUMERIC_COLUMNS = [
    "latitude",
    "longitude",
    "minimum_nights",
    "number_of_reviews",
    "reviews_per_month",
    "calculated_host_listings_count",
    "availability_365",
    "number_of_reviews_ltm",
]

PREFER_FEB_FEATURES = [
    "price",
    "room_type",
    "neighbourhood",
    "neighbourhood_group",
    "latitude",
    "longitude",
    "minimum_nights",
    "calculated_host_listings_count",
    "availability_365",
    "license",
]

MAX_FEATURES = [
    "number_of_reviews",
    "number_of_reviews_ltm",
    "reviews_last_365d",
]

MEAN_FEATURES = [
    "reviews_per_month",
    "reviews_last_30d",
    "reviews_last_90d",
]

MIN_FEATURES = [
    "days_since_last_review",
]

COUNT_FEATURES = [
    "number_of_reviews",
    "number_of_reviews_ltm",
    "reviews_last_30d",
    "reviews_last_90d",
    "reviews_last_365d",
]

FLOAT_COLUMNS = [
    "price",
    "latitude",
    "longitude",
    "reviews_per_month",
]

FINAL_COLUMNS = [
    "id",
    "price",
    "room_type",
    "neighbourhood",
    "neighbourhood_group",
    "latitude",
    "longitude",
    "minimum_nights",
    "number_of_reviews",
    "reviews_per_month",
    "calculated_host_listings_count",
    "availability_365",
    "number_of_reviews_ltm",
    "reviews_last_30d",
    "reviews_last_90d",
    "reviews_last_365d",
    "days_since_last_review",
    "is_licensed",
    "has_reviews",
]

COLLAPSE_RULES = {
    collapse_name: features
    for collapse_name, features in [
        ("prefer_feb", PREFER_FEB_FEATURES),
        ("max", MAX_FEATURES),
        ("mean", MEAN_FEATURES),
        ("min", MIN_FEATURES),
    ]
}


def coerce_id(series):
    return pd.to_numeric(series, errors="coerce").astype("Int64")


def coerce_numeric(series):
    return pd.to_numeric(series, errors="coerce")


def coerce_price(series):
    cleaned = (
        series.astype("string")
        .str.replace(r"[\$,]", "", regex=True)
        .str.strip()
        .replace({"": pd.NA, "nan": pd.NA, "<NA>": pd.NA})
    )
    return pd.to_numeric(cleaned, errors="coerce")


def load_listings(path, month_label):
    df = pd.read_csv(path)
    df["id"] = coerce_id(df["id"])
    df = df.dropna(subset=["id"]).copy()

    for column in LISTING_FEATURES:
        if column not in df.columns:
            df[column] = pd.NA

    df["price"] = coerce_price(df["price"])

    for column in LISTING_NUMERIC_COLUMNS:
        df[column] = coerce_numeric(df[column])

    df = df[["id", *LISTING_FEATURES]].drop_duplicates(subset="id", keep="last")
    return df.rename(columns={column: f"{column}_{month_label}" for column in LISTING_FEATURES})


def load_reviews(path):
    df = pd.read_csv(path, parse_dates=["date"])
    df["listing_id"] = coerce_id(df["listing_id"])
    return df.dropna(subset=["listing_id", "date"]).copy()


def aggregate_reviews(df, cutoff, month_label):
    df = df[df["date"] <= cutoff].copy()

    grouped = (
        df.groupby("listing_id")
        .agg(
            last_review_date=("date", "max"),
        )
        .reset_index()
    )

    for days in REVIEW_WINDOWS:
        window_start = cutoff - pd.Timedelta(days=days - 1)
        recent = (
            df[df["date"].between(window_start, cutoff)]
            .groupby("listing_id")
            .size()
            .rename(f"reviews_last_{days}d")
            .reset_index()
        )
        grouped = grouped.merge(recent, on="listing_id", how="left")

    for column in [f"reviews_last_{days}d" for days in REVIEW_WINDOWS]:
        grouped[column] = grouped[column].fillna(0)

    grouped["days_since_last_review"] = (cutoff - grouped["last_review_date"]).dt.days
    grouped = grouped.drop(columns="last_review_date")
    grouped = grouped.rename(columns={"listing_id": "id"})

    return grouped.rename(
        columns={column: f"{column}_{month_label}" for column in grouped.columns if column != "id"}
    )


def collapse_prefer_feb(df, feature):
    return df[f"{feature}_feb"].combine_first(df[f"{feature}_jan"])


def collapse_max(df, feature):
    return df[[f"{feature}_jan", f"{feature}_feb"]].max(axis=1, skipna=True)


def collapse_mean(df, feature):
    return df[[f"{feature}_jan", f"{feature}_feb"]].mean(axis=1, skipna=True)


def collapse_min(df, feature):
    return df[[f"{feature}_jan", f"{feature}_feb"]].min(axis=1, skipna=True)


COLLAPSE_FUNCTIONS = {
    "prefer_feb": collapse_prefer_feb,
    "max": collapse_max,
    "mean": collapse_mean,
    "min": collapse_min,
}


def cast_nullable_int(df, columns):
    for column in columns:
        df[column] = pd.to_numeric(df[column], errors="coerce").round().astype("Int64")


def build_presence_counts(merge_indicator):
    return {
        "jan_only": int(merge_indicator.eq("left_only").sum()),
        "feb_only": int(merge_indicator.eq("right_only").sum()),
        "both": int(merge_indicator.eq("both").sum()),
    }


def build_market_dataset():
    jan_listings = load_listings(JAN_LISTINGS_PATH, "jan")
    feb_listings = load_listings(FEB_LISTINGS_PATH, "feb")

    jan_reviews = load_reviews(JAN_REVIEWS_PATH)
    feb_reviews = load_reviews(FEB_REVIEWS_PATH)
    jan_reviews_agg = aggregate_reviews(jan_reviews, pd.Timestamp("2026-01-31"), "jan")
    feb_reviews_agg = aggregate_reviews(feb_reviews, pd.Timestamp("2026-02-28"), "feb")

    market_df = jan_listings.merge(feb_listings, on="id", how="outer", indicator=True)
    presence_counts = build_presence_counts(market_df["_merge"])
    market_df = market_df.drop(columns="_merge")

    market_df = market_df.merge(jan_reviews_agg, on="id", how="left")
    market_df = market_df.merge(feb_reviews_agg, on="id", how="left")

    review_count_columns = [
        *(
            f"reviews_last_{days}d_{month}"
            for days in REVIEW_WINDOWS
            for month in ("jan", "feb")
        ),
    ]
    market_df[review_count_columns] = market_df[review_count_columns].fillna(0)

    output = market_df[["id"]].copy()

    for rule_name, features in COLLAPSE_RULES.items():
        collapse_fn = COLLAPSE_FUNCTIONS[rule_name]
        for feature in features:
            output[feature] = collapse_fn(market_df, feature)

    cast_nullable_int(
        output,
        [
            "id",
            "minimum_nights",
            "calculated_host_listings_count",
            "availability_365",
            "days_since_last_review",
            *COUNT_FEATURES,
        ],
    )
    for column in FLOAT_COLUMNS:
        output[column] = coerce_numeric(output[column])

    output["is_licensed"] = output["license"].notna().astype(int)
    output["has_reviews"] = output["number_of_reviews"].fillna(0).gt(0).astype(int)
    output = output[FINAL_COLUMNS].sort_values("id").reset_index(drop=True)

    if output["id"].duplicated().any():
        raise ValueError("Duplicate listing ids found in final dataset.")

    month_specific_columns = {
        column for column in output.columns if column.endswith(("_jan", "_feb"))
    }
    if month_specific_columns:
        raise ValueError("Month-specific columns remain in final dataset.")

    return output, presence_counts


def main():
    market_df, presence_counts = build_market_dataset()
    market_df.to_csv(OUTPUT_PATH, index=False)


if __name__ == "__main__":
    main()
