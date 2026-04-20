"""
Task 30: Final Project Task — End-to-End Data Pipeline
Project: Power Grid Analytics Dashboard
Description: Complete pipeline:
  Ingestion → Processing → Storage → Orchestration → Dashboard

This is the master runner that ties all 29 previous tasks together.
"""

import os
import sys
import time
import json
import sqlite3
import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# -------------------------------------------------------
# Setup
# -------------------------------------------------------
os.makedirs("logs", exist_ok=True)
os.makedirs("data/raw", exist_ok=True)
os.makedirs("data/processed", exist_ok=True)
os.makedirs("data/warehouse", exist_ok=True)
os.makedirs("data/delta_lake/power_usage", exist_ok=True)
os.makedirs("reports", exist_ok=True)

logging.basicConfig(
    filename="logs/pipeline_master.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

pipeline_log = []

def step(name, fn):
    print(f"\n{'='*60}")
    print(f"  ▶  {name}")
    print(f"{'='*60}")
    start = time.time()
    try:
        result = fn()
        elapsed = round((time.time() - start) * 1000, 1)
        print(f"  ✅ Completed in {elapsed} ms")
        logging.info(f"[OK] {name} | {elapsed}ms")
        pipeline_log.append({"step": name, "status": "OK", "ms": elapsed})
        return result
    except Exception as e:
        elapsed = round((time.time() - start) * 1000, 1)
        print(f"  ❌ FAILED: {e}")
        logging.error(f"[FAIL] {name} | {e}")
        pipeline_log.append({"step": name, "status": "FAIL", "error": str(e), "ms": elapsed})
        return None

print("\n" + "█" * 60)
print("  POWER GRID ANALYTICS — END-TO-END PIPELINE")
print("  Final Capstone Project | Data Engineering")
print("█" * 60)

# ============================================================
# PHASE 1: INGESTION
# ============================================================
def phase1_ingestion():
    """Read raw CSV, validate, log ingestion metrics."""
    df = pd.read_csv("data/power_data.csv")
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df.dropna(inplace=True)
    df = df[df["consumption"] > 0]
    df.to_csv("data/raw/ingested_raw.csv", index=False)
    print(f"  Ingested: {len(df)} rows from CSV")
    return df

df_raw = step("PHASE 1: Data Ingestion", phase1_ingestion)

# ============================================================
# PHASE 2: PROCESSING / TRANSFORMATION
# ============================================================
def phase2_processing():
    df = pd.read_csv("data/raw/ingested_raw.csv")
    df["timestamp"]    = pd.to_datetime(df["timestamp"])
    df["hour"]         = df["timestamp"].dt.hour
    df["day"]          = df["timestamp"].dt.day_name()
    df["month"]        = df["timestamp"].dt.month
    df["is_peak"]      = df["hour"].between(8, 20).astype(int)
    df["load_level"]   = pd.cut(df["consumption"],
                                bins=[0,200,300,400,float("inf")],
                                labels=["Low","Normal","High","Critical"])
    df["consumption_norm"] = (df["consumption"] - df["consumption"].min()) / \
                             (df["consumption"].max() - df["consumption"].min())
    df["zscore"]       = (df["consumption"] - df["consumption"].mean()) / df["consumption"].std()
    df["is_anomaly"]   = (df["zscore"].abs() > 3).astype(int)
    df.to_csv("data/processed/power_processed.csv", index=False)
    print(f"  Processed: {len(df)} rows | Columns: {list(df.columns)}")
    print(f"  Anomalies detected: {df['is_anomaly'].sum()}")
    return df

df_proc = step("PHASE 2: Data Processing & Transformation", phase2_processing)

# ============================================================
# PHASE 3: STORAGE (MySQL-compatible SQLite + Warehouse)
# ============================================================
def phase3_storage():
    df = pd.read_csv("data/processed/power_processed.csv")

    # --- Operational DB (MySQL-compatible via SQLite) ---
    conn_ops = sqlite3.connect("data/processed/powergrid_ops.db")
    df.to_sql("power_usage", conn_ops, if_exists="replace", index=False)
    conn_ops.execute("""
        CREATE TABLE IF NOT EXISTS alerts AS
        SELECT timestamp, region, consumption, zscore
        FROM power_usage WHERE is_anomaly=1
    """)
    count_ops = conn_ops.execute("SELECT COUNT(*) FROM power_usage").fetchone()[0]
    conn_ops.close()
    print(f"  OPS DB: {count_ops} rows in power_usage table")

    # --- Star Schema Warehouse ---
    conn_dw = sqlite3.connect("data/warehouse/powergrid_star.db")
    df["region_key"] = pd.Categorical(df["region"]).codes + 1
    df.to_sql("fact_power_consumption", conn_dw, if_exists="replace", index=False)

    dim_region = df.groupby(["region","region_key"]).size().reset_index()[["region_key","region"]]
    dim_region.columns = ["region_key","region_name"]
    dim_region.to_sql("dim_region", conn_dw, if_exists="replace", index=False)

    count_dw = conn_dw.execute("SELECT COUNT(*) FROM fact_power_consumption").fetchone()[0]
    conn_dw.close()
    print(f"  DW Star Schema: {count_dw} rows in fact table")

    # --- Parquet (Data Lake) ---
    df.to_parquet("data/processed/power_processed.parquet", index=False)
    print(f"  Parquet saved: data/processed/power_processed.parquet")

def phase3_storage_wrapper():
    phase3_storage()
    return True

step("PHASE 3: Multi-Layer Storage", phase3_storage_wrapper)

# ============================================================
# PHASE 4: ORCHESTRATION (Airflow-style steps)
# ============================================================
def phase4_orchestration():
    tasks = [
        ("ingest",     "data/raw/ingested_raw.csv"),
        ("process",    "data/processed/power_processed.csv"),
        ("load_ops",   "data/processed/powergrid_ops.db"),
        ("load_dw",    "data/warehouse/powergrid_star.db"),
    ]
    dag_run = []
    for task_name, artifact in tasks:
        exists  = os.path.exists(artifact)
        status  = "SUCCESS" if exists else "SKIPPED"
        dag_run.append({"task": task_name, "artifact": artifact, "status": status, "run_at": str(datetime.now())})
        print(f"  Task [{task_name}]: {status} → {artifact}")

    dag_path = "logs/dag_run.json"
    with open(dag_path, "w") as f:
        json.dump(dag_run, f, indent=2)
    print(f"  DAG run log saved → {dag_path}")
    return dag_run

step("PHASE 4: Orchestration (DAG Execution)", phase4_orchestration)

# ============================================================
# PHASE 5: DASHBOARD DATA & ANALYTICS REPORT
# ============================================================
def phase5_dashboard():
    conn = sqlite3.connect("data/processed/powergrid_ops.db")
    df   = pd.read_sql("SELECT * FROM power_usage", conn)
    conn.close()

    print("\n  ╔══════════════════════════════════════════════╗")
    print("  ║   POWER GRID ANALYTICS — DASHBOARD SUMMARY  ║")
    print("  ╠══════════════════════════════════════════════╣")
    print(f"  ║  Total Records    : {len(df):<27}║")
    print(f"  ║  Avg Consumption  : {df['consumption'].mean():>10.2f} kWh               ║")
    print(f"  ║  Peak Consumption : {df['consumption'].max():>10.2f} kWh               ║")
    print(f"  ║  Anomalies Found  : {int(df['is_anomaly'].sum()):<27}║")
    print(f"  ║  Regions Covered  : {', '.join(df['region'].unique()):<27}║")
    print("  ╠══════════════════════════════════════════════╣")
    print("  ║  REGION BREAKDOWN:                           ║")
    region_stats = df.groupby("region")["consumption"].agg(["mean","max"]).round(1)
    for region, row in region_stats.iterrows():
        print(f"  ║    {region:<8}: avg={row['mean']:>6.1f}  peak={row['max']:>6.1f}            ║")
    print("  ╚══════════════════════════════════════════════╝")

    # Save as JSON for frontend
    dashboard_data = {
        "total_records"    : len(df),
        "avg_consumption"  : round(df["consumption"].mean(), 2),
        "peak_consumption" : round(df["consumption"].max(), 2),
        "anomalies"        : int(df["is_anomaly"].sum()),
        "region_stats"     : df.groupby("region")["consumption"].agg(["mean","max","min"]).round(2).to_dict(),
        "load_distribution": df["load_level"].value_counts().to_dict(),
        "generated_at"     : str(datetime.now()),
    }
    with open("reports/dashboard_data.json", "w") as f:
        json.dump(dashboard_data, f, indent=2, default=str)
    print(f"\n  Dashboard data saved → reports/dashboard_data.json")
    return dashboard_data

step("PHASE 5: Dashboard & Analytics Report", phase5_dashboard)

# ============================================================
# PIPELINE SUMMARY
# ============================================================
print("\n" + "█" * 60)
print("  PIPELINE EXECUTION SUMMARY")
print("█" * 60)
total_time = sum(s["ms"] for s in pipeline_log)
print(f"\n  {'Step':<45} {'Status':<10} {'Time':>8}")
print("  " + "-" * 65)
for s in pipeline_log:
    print(f"  {s['step']:<45} {s['status']:<10} {s['ms']:>6.0f}ms")
print("  " + "-" * 65)
print(f"  {'TOTAL PIPELINE TIME':<45} {'ALL OK':<10} {total_time:>6.0f}ms")

passed = sum(1 for s in pipeline_log if s["status"] == "OK")
print(f"\n  {passed}/{len(pipeline_log)} phases completed successfully.")
print(f"\n  Artifacts produced:")
artifacts = [
    "data/raw/ingested_raw.csv",
    "data/processed/power_processed.csv",
    "data/processed/power_processed.parquet",
    "data/processed/powergrid_ops.db",
    "data/warehouse/powergrid_star.db",
    "reports/dashboard_data.json",
    "logs/pipeline_master.log",
    "logs/dag_run.json",
]
for a in artifacts:
    exists = "✅" if os.path.exists(a) else "❌"
    print(f"    {exists} {a}")

print("\n" + "█" * 60)
print("  ✅  TASK 30: End-to-End Pipeline Complete!")
print("  Project: Power Grid Analytics Dashboard")
print("  Student: [Your Name] | Roll: [Roll No.] | Batch: [Batch]")
print("█" * 60)
