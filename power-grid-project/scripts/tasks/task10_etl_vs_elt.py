"""
Task 10: ETL vs ELT Task
Project: Power Grid Analytics Dashboard
Description: Build both ETL and ELT pipelines and compare performance.
"""

import pandas as pd
import numpy as np
import sqlite3
import time
import os

print("=" * 60)
print("  Task 10: ETL vs ELT Pipelines - Power Grid Data")
print("=" * 60)

os.makedirs("data/processed", exist_ok=True)
SOURCE_CSV = "data/power_data.csv"

# Generate source data if not present
if not os.path.exists(SOURCE_CSV):
    np.random.seed(0)
    N = 500
    regions = ["North","South","East","West"]
    from datetime import datetime, timedelta
    base = datetime(2026, 4, 1)
    rows = [{"timestamp": str(base + timedelta(minutes=i)),
             "region": np.random.choice(regions),
             "consumption": round(np.random.uniform(50,600),2),
             "voltage": round(np.random.uniform(210,245),1)} for i in range(N)]
    pd.DataFrame(rows).to_csv(SOURCE_CSV, index=False)

# ============================================================
# ETL PIPELINE: Extract → Transform → Load
# ============================================================
print("\n" + "="*50)
print("  ETL PIPELINE")
print("="*50)

etl_start = time.time()

# --- EXTRACT ---
print("\n[ETL] Step 1: EXTRACT - Reading raw CSV...")
df_raw = pd.read_csv(SOURCE_CSV)
print(f"  Extracted {len(df_raw)} rows, {df_raw.shape[1]} columns")

# --- TRANSFORM ---
print("[ETL] Step 2: TRANSFORM - Cleaning and enriching data...")

df_etl = df_raw.copy()
df_etl["timestamp"] = pd.to_datetime(df_etl["timestamp"])
df_etl.dropna(inplace=True)
df_etl["hour"]         = df_etl["timestamp"].dt.hour
df_etl["day"]          = df_etl["timestamp"].dt.day
df_etl["month"]        = df_etl["timestamp"].dt.month
df_etl["is_peak"]      = df_etl["hour"].between(8, 20).astype(int)
df_etl["load_level"]   = pd.cut(df_etl["consumption"],
                                bins=[0,200,300,400,float("inf")],
                                labels=["Low","Normal","High","Critical"])
df_etl["consumption"]  = df_etl["consumption"].round(2)

# Aggregation before loading
region_agg = df_etl.groupby("region")["consumption"].agg(["mean","max","min"]).reset_index()
region_agg.columns = ["region","avg_kwh","max_kwh","min_kwh"]

print(f"  Transformed: added 4 columns, aggregated region stats")

# --- LOAD ---
print("[ETL] Step 3: LOAD - Writing to SQLite...")
conn_etl = sqlite3.connect("data/processed/etl_output.db")
df_etl.to_sql("power_consumption_etl", conn_etl, if_exists="replace", index=False)
region_agg.to_sql("region_stats_etl", conn_etl, if_exists="replace", index=False)
conn_etl.close()

etl_time = time.time() - etl_start
print(f"  Loaded {len(df_etl)} rows into etl_output.db")
print(f"  ⏱  ETL Total Time: {etl_time*1000:.1f} ms")

# ============================================================
# ELT PIPELINE: Extract → Load (raw) → Transform (in DB)
# ============================================================
print("\n" + "="*50)
print("  ELT PIPELINE")
print("="*50)

elt_start = time.time()

# --- EXTRACT ---
print("\n[ELT] Step 1: EXTRACT - Reading raw CSV...")
df_raw2 = pd.read_csv(SOURCE_CSV)
print(f"  Extracted {len(df_raw2)} rows")

# --- LOAD (raw, minimal processing) ---
print("[ELT] Step 2: LOAD - Loading raw data into SQLite as-is...")
conn_elt = sqlite3.connect("data/processed/elt_output.db")
df_raw2.to_sql("power_raw", conn_elt, if_exists="replace", index=False)
print(f"  Loaded {len(df_raw2)} raw rows into elt_output.db")

# --- TRANSFORM (inside DB using SQL) ---
print("[ELT] Step 3: TRANSFORM - Transforming data inside the database...")
conn_elt.execute("""
CREATE TABLE IF NOT EXISTS power_consumption_elt AS
SELECT
    timestamp,
    region,
    ROUND(consumption, 2) AS consumption,
    voltage,
    CAST(substr(timestamp, 12, 2) AS INTEGER) AS hour,
    CAST(substr(timestamp, 9, 2) AS INTEGER)  AS day,
    CAST(substr(timestamp, 6, 2) AS INTEGER)  AS month,
    CASE
        WHEN CAST(substr(timestamp, 12, 2) AS INTEGER) BETWEEN 8 AND 20 THEN 1
        ELSE 0
    END AS is_peak,
    CASE
        WHEN consumption < 200 THEN 'Low'
        WHEN consumption < 300 THEN 'Normal'
        WHEN consumption < 400 THEN 'High'
        ELSE 'Critical'
    END AS load_level
FROM power_raw
WHERE consumption IS NOT NULL AND region IS NOT NULL
""")

conn_elt.execute("""
CREATE TABLE IF NOT EXISTS region_stats_elt AS
SELECT region,
    ROUND(AVG(consumption),2) AS avg_kwh,
    ROUND(MAX(consumption),2) AS max_kwh,
    ROUND(MIN(consumption),2) AS min_kwh
FROM power_raw GROUP BY region
""")

conn_elt.commit()
conn_elt.close()

elt_time = time.time() - elt_start
print(f"  Transformed inside DB using SQL views/CTEs")
print(f"  ⏱  ELT Total Time: {elt_time*1000:.1f} ms")

# ============================================================
# COMPARISON TABLE
# ============================================================
print("\n" + "="*60)
print("  ETL vs ELT Comparison")
print("="*60)
comparison = [
    ("Approach",        "Extract→Transform→Load",     "Extract→Load→Transform"),
    ("Transformation",  "Before loading (in Python)",  "After loading (in SQL/DB)"),
    ("Flexibility",     "More control in code",        "Scalable with DB engine"),
    ("Use Case",        "Structured targets, small DW","Big data, cloud DW, lakes"),
    ("Time (this run)", f"{etl_time*1000:.1f} ms",     f"{elt_time*1000:.1f} ms"),
    ("Winner here",     "✅ If pre-processing needed",  "✅ If DB is powerful"),
]

print(f"\n  {'Aspect':<22} {'ETL':<35} {'ELT':<35}")
print("  " + "-"*90)
for row in comparison:
    print(f"  {row[0]:<22} {row[1]:<35} {row[2]:<35}")

print("\n✅ Task 10: ETL vs ELT Pipeline Comparison Complete.")
