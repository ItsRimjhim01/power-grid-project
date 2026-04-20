"""
Task 5: Pandas + NumPy Task
Project: Power Grid Analytics Dashboard
Description: Large-scale data analysis (1M+ rows), memory optimization,
             performance comparison.
"""

import pandas as pd
import numpy as np
import time
import os

print("=" * 60)
print("  Task 5: Pandas + NumPy - Large-Scale Analysis")
print("=" * 60)

# -------------------------------------------------------
# Step 1: Generate a large dataset (1M rows)
# -------------------------------------------------------
print("\n[+] Generating 1,000,000 row dataset...")
os.makedirs("data/processed", exist_ok=True)

np.random.seed(42)
N = 1_000_000
regions = np.array(["North", "South", "East", "West"])

df_large = pd.DataFrame({
    "region"     : np.random.choice(regions, N),
    "consumption": np.round(np.random.uniform(50, 600, N), 2),
    "voltage"    : np.round(np.random.uniform(210, 245, N), 1),
    "frequency"  : np.round(np.random.uniform(49.5, 50.5, N), 3),
    "hour"       : np.random.randint(0, 24, N),
})

large_path = "data/processed/power_large.csv"
df_large.to_csv(large_path, index=False)
print(f"  Dataset saved: {large_path}")
print(f"  Shape: {df_large.shape}  |  Memory: {df_large.memory_usage(deep=True).sum() / 1e6:.2f} MB")

# -------------------------------------------------------
# Step 2: Memory Optimization using category + float32
# -------------------------------------------------------
print("\n[+] Optimizing memory usage...")

def get_mem_mb(df):
    return df.memory_usage(deep=True).sum() / 1e6

mem_before = get_mem_mb(df_large)

df_opt = df_large.copy()
df_opt["region"]      = df_opt["region"].astype("category")
df_opt["consumption"] = df_opt["consumption"].astype("float32")
df_opt["voltage"]     = df_opt["voltage"].astype("float32")
df_opt["frequency"]   = df_opt["frequency"].astype("float32")
df_opt["hour"]        = df_opt["hour"].astype("int8")

mem_after = get_mem_mb(df_opt)
print(f"  Before optimization: {mem_before:.2f} MB")
print(f"  After  optimization: {mem_after:.2f} MB")
print(f"  Saved             : {mem_before - mem_after:.2f} MB ({(1 - mem_after/mem_before)*100:.1f}% reduction)")

# -------------------------------------------------------
# Step 3: Performance Comparison - Pandas vs NumPy
# -------------------------------------------------------
print("\n[+] Performance Comparison: Pandas vs NumPy")
consumption_array = df_opt["consumption"].values

# Pandas groupby
start = time.time()
region_avg_pandas = df_opt.groupby("region")["consumption"].mean()
pandas_time = time.time() - start

# NumPy manual groupby
start = time.time()
region_labels = df_opt["region"].cat.codes.values
unique_regions = df_opt["region"].cat.categories.tolist()
numpy_avgs = {
    r: np.mean(consumption_array[region_labels == i])
    for i, r in enumerate(unique_regions)
}
numpy_time = time.time() - start

print(f"\n  Pandas groupby mean  : {pandas_time*1000:.1f} ms")
print(f"  NumPy manual groupby : {numpy_time*1000:.1f} ms")

print("\n  Region-wise average consumption (Pandas):")
print(region_avg_pandas.round(2).to_string())

# -------------------------------------------------------
# Step 4: Statistical Summary with NumPy
# -------------------------------------------------------
print("\n[+] NumPy Statistical Analysis on consumption:")
arr = consumption_array
print(f"  Mean     : {np.mean(arr):.2f}")
print(f"  Std Dev  : {np.std(arr):.2f}")
print(f"  Min      : {np.min(arr):.2f}")
print(f"  Max      : {np.max(arr):.2f}")
print(f"  Median   : {np.median(arr):.2f}")
print(f"  95th pct : {np.percentile(arr, 95):.2f}")
print(f"  99th pct : {np.percentile(arr, 99):.2f}")

# -------------------------------------------------------
# Step 5: Detect outliers using IQR
# -------------------------------------------------------
print("\n[+] Outlier Detection (IQR Method):")
Q1 = np.percentile(arr, 25)
Q3 = np.percentile(arr, 75)
IQR = Q3 - Q1
lower = Q1 - 1.5 * IQR
upper = Q3 + 1.5 * IQR
outliers = arr[(arr < lower) | (arr > upper)]
print(f"  Q1={Q1:.2f}, Q3={Q3:.2f}, IQR={IQR:.2f}")
print(f"  Lower bound: {lower:.2f} | Upper bound: {upper:.2f}")
print(f"  Outliers found: {len(outliers)} ({len(outliers)/N*100:.2f}%)")

print("\n✅ Task 5: Pandas + NumPy Large-Scale Analysis Complete.")
