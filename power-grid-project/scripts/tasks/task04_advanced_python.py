"""
Task 4: Advanced Python Task
Project: Power Grid Analytics Dashboard
Description: Reusable modules for normalization, aggregation, and validation.
"""

import pandas as pd
import numpy as np
import os

# ================================================================
# MODULE 1: Normalization
# ================================================================
class Normalizer:
    """Normalizes numerical columns in a DataFrame."""

    def min_max(self, df: pd.DataFrame, col: str) -> pd.DataFrame:
        """Min-Max normalization to [0, 1] range."""
        df = df.copy()
        mn, mx = df[col].min(), df[col].max()
        df[f"{col}_normalized"] = (df[col] - mn) / (mx - mn) if mx != mn else 0.0
        return df

    def z_score(self, df: pd.DataFrame, col: str) -> pd.DataFrame:
        """Z-score standardization (mean=0, std=1)."""
        df = df.copy()
        mean, std = df[col].mean(), df[col].std()
        df[f"{col}_zscore"] = (df[col] - mean) / std if std != 0 else 0.0
        return df


# ================================================================
# MODULE 2: Aggregation
# ================================================================
class Aggregator:
    """Aggregates power grid data by time or region."""

    def region_summary(self, df: pd.DataFrame) -> pd.DataFrame:
        """Average, max, min consumption per region."""
        return df.groupby("region")["consumption"].agg(
            avg_consumption="mean",
            max_consumption="max",
            min_consumption="min",
            total_records="count"
        ).reset_index().round(2)

    def hourly_trend(self, df: pd.DataFrame) -> pd.DataFrame:
        """Hourly average consumption."""
        df = df.copy()
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df["hour"] = df["timestamp"].dt.hour
        return df.groupby("hour")["consumption"].mean().reset_index().rename(
            columns={"consumption": "avg_consumption"}
        ).round(2)

    def daily_trend(self, df: pd.DataFrame) -> pd.DataFrame:
        """Daily total and average consumption."""
        df = df.copy()
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df["date"] = df["timestamp"].dt.date
        return df.groupby("date")["consumption"].agg(
            total_consumption="sum",
            avg_consumption="mean"
        ).reset_index().round(2)


# ================================================================
# MODULE 3: Validation
# ================================================================
class Validator:
    """Validates Power Grid data quality."""

    def check_missing(self, df: pd.DataFrame) -> dict:
        missing = df.isnull().sum()
        return missing[missing > 0].to_dict()

    def check_range(self, df: pd.DataFrame, col: str, low: float, high: float) -> pd.DataFrame:
        """Return rows where values are out of expected range."""
        return df[(df[col] < low) | (df[col] > high)]

    def check_duplicates(self, df: pd.DataFrame) -> int:
        return df.duplicated().sum()

    def full_report(self, df: pd.DataFrame) -> None:
        print("\n📋 Validation Report")
        print("-" * 40)
        print(f"  Total Rows       : {len(df)}")
        print(f"  Duplicate Rows   : {self.check_duplicates(df)}")
        missing = self.check_missing(df)
        if missing:
            print(f"  Missing Values   : {missing}")
        else:
            print(f"  Missing Values   : None ✅")
        out_of_range = self.check_range(df, "consumption", 50, 600)
        print(f"  Out-of-Range (consumption 50–600): {len(out_of_range)} rows")
        print("-" * 40)


# ================================================================
# DEMO: Run all modules on sample data
# ================================================================
if __name__ == "__main__":
    print("=" * 60)
    print("  Task 4: Advanced Python - Reusable Transformation Modules")
    print("=" * 60)

    # Load sample data
    df = pd.read_csv("data/power_data.csv")
    df.columns = df.columns.str.strip()

    print(f"\n  Loaded: {len(df)} rows\n")

    # --- Normalizer ---
    norm = Normalizer()
    df = norm.min_max(df, "consumption")
    df = norm.z_score(df, "consumption")
    print("  [Normalizer] Min-Max + Z-Score applied.")
    print(df[["consumption", "consumption_normalized", "consumption_zscore"]].head(5).to_string(index=False))

    # --- Aggregator ---
    agg = Aggregator()
    region_summary = agg.region_summary(df)
    print("\n  [Aggregator] Region Summary:")
    print(region_summary.to_string(index=False))

    hourly = agg.hourly_trend(df)
    print("\n  [Aggregator] Hourly Trend (first 5):")
    print(hourly.head(5).to_string(index=False))

    # --- Validator ---
    val = Validator()
    val.full_report(df)

    # Save results
    os.makedirs("data/processed", exist_ok=True)
    df.to_csv("data/processed/power_transformed.csv", index=False)
    region_summary.to_csv("data/processed/region_summary.csv", index=False)
    print("\n  Saved transformed data → data/processed/")
    print("\n✅ Task 4: Advanced Python Modules Complete.")
