"""
Task 3: Python Basics Task
Project: Power Grid Analytics Dashboard
Description: Read multiple CSV files, clean missing values, merge datasets.
"""

import pandas as pd
import os
import random
from datetime import datetime, timedelta

print("=" * 60)
print("  Task 3: Python Basics - Read, Clean, Merge CSVs")
print("=" * 60)

# -------------------------------------------------------
# Step 1: Generate multiple sample CSV files
# -------------------------------------------------------
os.makedirs("data/raw", exist_ok=True)
regions = ["North", "South", "East", "West"]
base_time = datetime(2026, 4, 1, 0, 0, 0)

for region in regions:
    rows = []
    for i in range(30):
        # Randomly introduce missing values (10% chance)
        consumption = round(random.uniform(100, 500), 2) if random.random() > 0.1 else None
        region_val  = region if random.random() > 0.05 else None
        rows.append({
            "timestamp"  : base_time + timedelta(hours=i),
            "region"     : region_val,
            "consumption": consumption,
            "voltage"    : round(random.uniform(220, 240), 1) if random.random() > 0.1 else None
        })
    df = pd.DataFrame(rows)
    path = f"data/raw/power_{region.lower()}.csv"
    df.to_csv(path, index=False)
    print(f"  Created: {path}  ({len(df)} rows)")

# -------------------------------------------------------
# Step 2: Read all CSV files
# -------------------------------------------------------
print("\n[+] Reading all region CSV files...")
all_dfs = []
for region in regions:
    path = f"data/raw/power_{region.lower()}.csv"
    df   = pd.read_csv(path)
    print(f"  {path}: {df.shape[0]} rows, {df.shape[1]} cols | Missing: {df.isnull().sum().sum()}")
    all_dfs.append(df)

# -------------------------------------------------------
# Step 3: Merge all datasets
# -------------------------------------------------------
print("\n[+] Merging all datasets...")
merged = pd.concat(all_dfs, ignore_index=True)
print(f"  Merged shape: {merged.shape}")
print(f"  Missing values before cleaning:\n{merged.isnull().sum()}")

# -------------------------------------------------------
# Step 4: Clean missing values
# -------------------------------------------------------
print("\n[+] Cleaning missing values...")

# Fill missing consumption with region median
merged["consumption"] = merged.groupby("region")["consumption"].transform(
    lambda x: x.fillna(x.median())
)

# Fill missing voltage with global median
merged["voltage"] = merged["voltage"].fillna(merged["voltage"].median())

# Drop rows where region is still null
before = len(merged)
merged.dropna(subset=["region"], inplace=True)
after  = len(merged)
print(f"  Dropped {before - after} rows with missing region.")

# Reset index
merged.reset_index(drop=True, inplace=True)
merged["timestamp"] = pd.to_datetime(merged["timestamp"])
merged.sort_values("timestamp", inplace=True)

print(f"  Missing values after cleaning:\n{merged.isnull().sum()}")
print(f"  Final dataset shape: {merged.shape}")

# -------------------------------------------------------
# Step 5: Save merged + cleaned dataset
# -------------------------------------------------------
os.makedirs("data/processed", exist_ok=True)
out_path = "data/processed/power_merged_clean.csv"
merged.to_csv(out_path, index=False)
print(f"\n  Saved cleaned merged dataset → {out_path}")
print("\n  Preview:")
print(merged.head(5).to_string(index=False))

print("\n✅ Task 3: Python Basics Complete.")
