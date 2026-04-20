"""
Task 28: Lakehouse Task
Project: Power Grid Analytics Dashboard
Description: Implement Delta Lake/Iceberg concepts - versioning,
             ACID transactions, schema evolution, time travel.
"""

import pandas as pd
import numpy as np
import json
import os
import shutil
from datetime import datetime

print("=" * 60)
print("  Task 28: Lakehouse Architecture - Delta Lake Simulation")
print("=" * 60)

print("""
  Lakehouse Architecture for Power Grid:
  =======================================
  Traditional:  Data Lake (raw)  +  Data Warehouse (structured)
  Lakehouse  :  Single platform with ACID + SQL + versioning

  Delta Lake adds to Parquet files:
  ├── _delta_log/            ← transaction log (JSON files)
  │   ├── 00000.json         ← commit 0: initial write
  │   ├── 00001.json         ← commit 1: update/append
  │   └── 00002.json         ← commit 2: delete/schema change
  └── part-*.parquet         ← actual data files
""")

# -------------------------------------------------------
# Simulate Delta Lake structure locally
# -------------------------------------------------------
DELTA_PATH = "data/delta_lake/power_usage"
LOG_PATH   = f"{DELTA_PATH}/_delta_log"
shutil.rmtree(DELTA_PATH, ignore_errors=True)
os.makedirs(LOG_PATH, exist_ok=True)

commit_version = 0

def delta_commit(operation, df_new, description=""):
    """Simulate a Delta Lake commit (append/overwrite)."""
    global commit_version

    # Save data as parquet
    parquet_path = f"{DELTA_PATH}/part-{commit_version:05d}.parquet"
    df_new.to_parquet(parquet_path, index=False)

    # Write transaction log
    log_entry = {
        "commitInfo": {
            "timestamp"  : str(datetime.now()),
            "operation"  : operation,
            "description": description,
            "version"    : commit_version,
        },
        "add": {
            "path"             : f"part-{commit_version:05d}.parquet",
            "size"             : os.path.getsize(parquet_path),
            "numRecords"       : len(df_new),
            "partitionValues"  : {},
        }
    }
    log_file = f"{LOG_PATH}/{commit_version:020d}.json"
    with open(log_file, "w") as f:
        json.dump(log_entry, f, indent=2)

    print(f"  ✅ Commit {commit_version}: [{operation}] — {len(df_new)} rows, {description}")
    commit_version += 1
    return commit_version - 1

def delta_read_version(version=None):
    """Simulate Delta Lake time travel: read as of version N."""
    versions_to_read = range(version + 1) if version is not None else range(commit_version)
    all_dfs = []
    for v in versions_to_read:
        parquet_path = f"{DELTA_PATH}/part-{v:05d}.parquet"
        if os.path.exists(parquet_path):
            all_dfs.append(pd.read_parquet(parquet_path))
    if all_dfs:
        df_combined = pd.concat(all_dfs, ignore_index=True)
        return df_combined
    return pd.DataFrame()

# -------------------------------------------------------
# Commit 0: Initial Write
# -------------------------------------------------------
print("\n[+] Commit 0: Initial Write (WRITE operation)")
np.random.seed(42)
df_v0 = pd.read_csv("data/power_data.csv")
df_v0["timestamp"] = pd.to_datetime(df_v0["timestamp"]).astype(str)
v0 = delta_commit("WRITE", df_v0, "Initial load from CSV")

# -------------------------------------------------------
# Commit 1: Append new records
# -------------------------------------------------------
print("\n[+] Commit 1: Append new records (APPEND operation)")
from datetime import timedelta
base = datetime(2026, 4, 18)
new_rows = pd.DataFrame([{
    "timestamp"  : str(base + timedelta(hours=i)),
    "region"     : np.random.choice(["North","South","East","West"]),
    "consumption": round(np.random.uniform(100, 500), 2)
} for i in range(10)])
v1 = delta_commit("APPEND", new_rows, "Streaming append — new sensor readings")

# -------------------------------------------------------
# Commit 2: Schema evolution (add voltage column)
# -------------------------------------------------------
print("\n[+] Commit 2: Schema Evolution (ADD COLUMN voltage)")
df_with_voltage = new_rows.copy()
df_with_voltage["voltage"] = np.round(np.random.uniform(220, 240, len(df_with_voltage)), 1)
v2 = delta_commit("SCHEMA_EVOLUTION", df_with_voltage, "Added voltage column")

# -------------------------------------------------------
# Time Travel: Read as of version 0
# -------------------------------------------------------
print(f"\n[+] Time Travel: Read data as of Version 0")
df_at_v0 = delta_read_version(version=0)
print(f"  Records at Version 0: {len(df_at_v0)}")
print(df_at_v0.head(3).to_string(index=False))

print(f"\n[+] Time Travel: Read data as of Version 1 (with appends)")
df_at_v1 = delta_read_version(version=1)
print(f"  Records at Version 1: {len(df_at_v1)}")

print(f"\n[+] Current (latest) snapshot:")
df_latest = delta_read_version()
print(f"  Total records: {len(df_latest)}")

# -------------------------------------------------------
# Display transaction log
# -------------------------------------------------------
print(f"\n[+] Delta Lake Transaction Log (_delta_log):")
for log_file in sorted(os.listdir(LOG_PATH)):
    with open(f"{LOG_PATH}/{log_file}") as f:
        entry = json.load(f)
    ci = entry["commitInfo"]
    ad = entry["add"]
    print(f"  Version {ci['version']}: [{ci['operation']}] — {ad['numRecords']} rows | {ci['description']}")

print(f"""
  Delta Lake Advantages Demonstrated:
  ✅ ACID Transactions — each commit is atomic
  ✅ Time Travel      — read any historical version
  ✅ Schema Evolution — new columns added safely
  ✅ Audit Log        — full history in _delta_log/
  ✅ Parquet format   — columnar, compressed, fast
  
  In production:
    from delta import DeltaTable
    DeltaTable.forPath(spark, "s3://bucket/power_usage/")
      .history()
      .show()
""")

print(f"\n  Delta Lake simulation → {DELTA_PATH}")
print("\n✅ Task 28: Lakehouse (Delta Lake) Complete.")
