"""
Task 29: Data Quality Task
Project: Power Grid Analytics Dashboard
Description: Build validation checks and anomaly detection system
             for power grid data.
"""

import pandas as pd
import numpy as np
import json
import os
import sqlite3
from datetime import datetime

print("=" * 60)
print("  Task 29: Data Quality & Anomaly Detection System")
print("=" * 60)

os.makedirs("logs", exist_ok=True)
os.makedirs("data/processed", exist_ok=True)

# -------------------------------------------------------
# Load data
# -------------------------------------------------------
df = pd.read_csv("data/power_data.csv")
df["timestamp"] = pd.to_datetime(df["timestamp"])

# Inject some artificial quality issues for demo
np.random.seed(10)
# Add nulls
df.loc[df.sample(3).index, "consumption"] = None
# Add duplicates
df = pd.concat([df, df.sample(2)], ignore_index=True)
# Add outliers
df.loc[len(df)] = [datetime.now(), "North", 9999.0]  # extreme spike
df.loc[len(df)] = [datetime.now(), "South", -50.0]   # negative value

print(f"\n  Dataset loaded: {len(df)} rows (includes injected quality issues)\n")

# ============================================================
# VALIDATION ENGINE
# ============================================================
class DataQualityValidator:
    def __init__(self, df: pd.DataFrame):
        self.df      = df.copy()
        self.issues  = []
        self.report  = {}

    def _flag(self, check, count, detail=""):
        status = "❌ FAIL" if count > 0 else "✅ PASS"
        self.issues.append({"check": check, "count": count, "status": status, "detail": detail})
        print(f"  {status}  [{check}] — {count} issues  {detail}")

    def check_nulls(self):
        for col in self.df.columns:
            n = self.df[col].isnull().sum()
            self._flag(f"NULL in '{col}'", n)

    def check_duplicates(self):
        n = self.df.duplicated().sum()
        self._flag("Duplicate rows", n)

    def check_range(self, col, low, high):
        mask = (self.df[col] < low) | (self.df[col] > high)
        n    = mask.sum()
        self._flag(f"Out-of-range '{col}' [{low},{high}]", n,
                   f"(values: {list(self.df.loc[mask, col].round(1).values[:3])})")

    def check_valid_categories(self, col, valid_set):
        mask = ~self.df[col].isin(valid_set)
        n    = mask.sum()
        self._flag(f"Invalid '{col}' categories", n,
                   f"(allowed: {valid_set})")

    def check_timestamp_order(self):
        out_of_order = (self.df["timestamp"].diff() < pd.Timedelta(0)).sum()
        self._flag("Timestamp out-of-order", out_of_order)

    def run_all(self):
        print("\n  Running Data Quality Checks...")
        print("  " + "-"*55)
        self.check_nulls()
        self.check_duplicates()
        self.check_range("consumption", 0, 600)
        self.check_valid_categories("region", {"North","South","East","West"})
        self.check_timestamp_order()
        print("  " + "-"*55)
        total_issues = sum(i["count"] for i in self.issues)
        total_checks = len(self.issues)
        passed = sum(1 for i in self.issues if i["count"] == 0)
        print(f"\n  Summary: {passed}/{total_checks} checks passed | {total_issues} total issues found")
        return self.issues

# ============================================================
# ANOMALY DETECTION ENGINE
# ============================================================
class AnomalyDetector:
    def __init__(self, df: pd.DataFrame):
        self.df       = df.dropna().copy()
        self.anomalies = pd.DataFrame()

    def zscore_detection(self, col, threshold=3.0):
        """Flag values more than N standard deviations from mean."""
        mean = self.df[col].mean()
        std  = self.df[col].std()
        self.df["zscore"] = (self.df[col] - mean).abs() / std
        flagged = self.df[self.df["zscore"] > threshold]
        print(f"\n  [Z-Score >{threshold}σ] Found {len(flagged)} anomalies in '{col}':")
        if len(flagged):
            print(flagged[["timestamp","region",col,"zscore"]].round(2).to_string(index=False))
        return flagged

    def iqr_detection(self, col):
        """IQR method for outlier detection."""
        Q1  = self.df[col].quantile(0.25)
        Q3  = self.df[col].quantile(0.75)
        IQR = Q3 - Q1
        lower = Q1 - 1.5 * IQR
        upper = Q3 + 1.5 * IQR
        flagged = self.df[(self.df[col] < lower) | (self.df[col] > upper)]
        print(f"\n  [IQR Method] Bounds: [{lower:.1f}, {upper:.1f}] | Found {len(flagged)} outliers:")
        if len(flagged):
            print(flagged[["timestamp","region",col]].round(2).to_string(index=False))
        return flagged

    def rolling_spike_detection(self, col, window=5, spike_factor=2.0):
        """Detect sudden spikes vs rolling average."""
        self.df["rolling_avg"] = self.df[col].rolling(window=window, min_periods=1).mean()
        self.df["spike_ratio"] = self.df[col] / self.df["rolling_avg"]
        flagged = self.df[self.df["spike_ratio"] > spike_factor]
        print(f"\n  [Spike Detection (>{spike_factor}x rolling avg)] Found {len(flagged)} spikes:")
        if len(flagged):
            print(flagged[["timestamp","region",col,"rolling_avg","spike_ratio"]].round(2).head(5).to_string(index=False))
        return flagged

    def regional_anomaly(self):
        """Flag regions whose average deviates far from overall average."""
        overall_avg = self.df["consumption"].mean()
        overall_std = self.df["consumption"].std()
        region_avgs = self.df.groupby("region")["consumption"].mean()
        print(f"\n  [Regional Anomaly] Overall avg: {overall_avg:.2f} ± {overall_std:.2f}")
        for region, avg in region_avgs.items():
            z = (avg - overall_avg) / overall_std
            flag = " ⚠️ ANOMALOUS" if abs(z) > 1.5 else " ✅ Normal"
            print(f"    {region:<8}: avg={avg:.2f}  z={z:+.2f}{flag}")

# ============================================================
# RUN EVERYTHING
# ============================================================
print("\n" + "="*50)
print("  DATA VALIDATION")
print("="*50)
validator = DataQualityValidator(df)
issues    = validator.run_all()

print("\n" + "="*50)
print("  ANOMALY DETECTION")
print("="*50)
detector  = AnomalyDetector(df)
z_anomalies      = detector.zscore_detection("consumption", threshold=3.0)
iqr_anomalies    = detector.iqr_detection("consumption")
spike_anomalies  = detector.rolling_spike_detection("consumption")
detector.regional_anomaly()

# ============================================================
# SAVE QUALITY REPORT
# ============================================================
report = {
    "generated_at"   : str(datetime.now()),
    "total_records"  : len(df),
    "validation"     : issues,
    "anomaly_summary": {
        "zscore_anomalies" : len(z_anomalies),
        "iqr_outliers"     : len(iqr_anomalies),
        "spike_events"     : len(spike_anomalies),
    }
}
report_path = "data/processed/data_quality_report.json"
with open(report_path, "w") as f:
    json.dump(report, f, indent=2, default=str)

print(f"\n  Quality report saved → {report_path}")
print("\n✅ Task 29: Data Quality & Anomaly Detection Complete.")
