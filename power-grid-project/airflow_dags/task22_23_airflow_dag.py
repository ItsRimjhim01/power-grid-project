"""
Tasks 22–23: Apache Airflow DAGs
Project: Power Grid Analytics Dashboard
Description:
  Task 22 - Basic DAG for ETL pipeline
  Task 23 - Advanced DAG with scheduling, monitoring, retry mechanisms
"""

# ============================================================
# NOTE: This file defines Airflow DAGs.
# To run: place in ~/airflow/dags/ and start Airflow scheduler.
# Requires: pip install apache-airflow
# For simulation without Airflow, see the bottom of this file.
# ============================================================

from datetime import datetime, timedelta

# -------------------------------------------------------
# Try to import Airflow; fall back to simulation if absent
# -------------------------------------------------------
try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from airflow.operators.bash import BashOperator
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False

import pandas as pd
import sqlite3
import os
import time
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
os.makedirs("logs", exist_ok=True)
os.makedirs("data/processed", exist_ok=True)

# ============================================================
# PIPELINE TASK FUNCTIONS (shared by Airflow & simulation)
# ============================================================

def extract_power_data(**kwargs):
    """Extract: Read CSV source data."""
    logging.info("[EXTRACT] Reading power_data.csv...")
    df = pd.read_csv("data/power_data.csv")
    logging.info(f"[EXTRACT] Loaded {len(df)} rows.")
    df.to_csv("data/processed/extract_stage.csv", index=False)
    return len(df)

def validate_data(**kwargs):
    """Validate: Check for nulls and out-of-range values."""
    logging.info("[VALIDATE] Running data quality checks...")
    df = pd.read_csv("data/processed/extract_stage.csv")
    null_count = df.isnull().sum().sum()
    neg_count  = (df["consumption"] < 0).sum()
    if null_count > 0:
        logging.warning(f"[VALIDATE] Found {null_count} null values — dropping.")
        df.dropna(inplace=True)
    if neg_count > 0:
        logging.warning(f"[VALIDATE] Found {neg_count} negative consumption rows — removing.")
        df = df[df["consumption"] >= 0]
    df.to_csv("data/processed/validated_stage.csv", index=False)
    logging.info(f"[VALIDATE] Validation complete. {len(df)} rows remain.")
    return len(df)

def transform_data(**kwargs):
    """Transform: Enrich data with derived columns."""
    logging.info("[TRANSFORM] Applying transformations...")
    df = pd.read_csv("data/processed/validated_stage.csv")
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["hour"]      = df["timestamp"].dt.hour
    df["day"]       = df["timestamp"].dt.day_name()
    df["is_peak"]   = df["hour"].between(8, 20).astype(int)
    df["load_level"] = pd.cut(df["consumption"],
        bins=[0, 200, 300, 400, float("inf")],
        labels=["Low", "Normal", "High", "Critical"])
    df["consumption_norm"] = (df["consumption"] - df["consumption"].min()) / \
                             (df["consumption"].max() - df["consumption"].min())
    df.to_csv("data/processed/transformed_stage.csv", index=False)
    logging.info(f"[TRANSFORM] Done. {len(df)} rows, {df.shape[1]} columns.")
    return len(df)

def load_to_db(**kwargs):
    """Load: Write transformed data to SQLite."""
    logging.info("[LOAD] Writing to SQLite database...")
    df  = pd.read_csv("data/processed/transformed_stage.csv")
    conn = sqlite3.connect("data/processed/powergrid_airflow.db")
    df.to_sql("power_usage_final", conn, if_exists="replace", index=False)
    count = conn.execute("SELECT COUNT(*) FROM power_usage_final").fetchone()[0]
    conn.close()
    logging.info(f"[LOAD] Loaded {count} rows into powergrid_airflow.db")
    return count

def generate_report(**kwargs):
    """Report: Create summary report."""
    logging.info("[REPORT] Generating analytics report...")
    conn = sqlite3.connect("data/processed/powergrid_airflow.db")
    df   = pd.read_sql("SELECT * FROM power_usage_final", conn)
    conn.close()
    summary = df.groupby("region")["consumption"].agg(["mean","max","min","count"]).round(2)
    report_path = "data/processed/pipeline_report.csv"
    summary.to_csv(report_path)
    logging.info(f"[REPORT] Report saved → {report_path}")
    print("\nPipeline Summary Report:")
    print(summary.to_string())

def send_alert(**kwargs):
    """Alert: Log any critical consumption values."""
    conn = sqlite3.connect("data/processed/powergrid_airflow.db")
    df   = pd.read_sql("SELECT * FROM power_usage_final WHERE load_level='Critical'", conn)
    conn.close()
    if len(df) > 0:
        logging.warning(f"[ALERT] {len(df)} CRITICAL consumption events detected!")
        with open("logs/alerts.log", "a") as f:
            f.write(f"\n[{datetime.now()}] ALERT: {len(df)} critical events\n")
            f.write(df[["timestamp","region","consumption"]].to_string())
    else:
        logging.info("[ALERT] No critical events. All clear.")

# ============================================================
# TASK 22: Basic DAG Definition
# ============================================================
if AIRFLOW_AVAILABLE:
    default_args_basic = {
        "owner"          : "power_grid_admin",
        "retries"        : 1,
        "retry_delay"    : timedelta(minutes=2),
        "start_date"     : datetime(2026, 4, 1),
    }

    with DAG(
        dag_id="task22_power_grid_etl_basic",
        default_args=default_args_basic,
        description="Basic ETL DAG for Power Grid Analytics",
        schedule_interval="@daily",
        catchup=False,
        tags=["power_grid", "etl", "basic"],
    ) as dag_basic:

        t_extract   = PythonOperator(task_id="extract",   python_callable=extract_power_data)
        t_transform = PythonOperator(task_id="transform", python_callable=transform_data)
        t_load      = PythonOperator(task_id="load",      python_callable=load_to_db)

        t_extract >> t_transform >> t_load

# ============================================================
# TASK 23: Advanced DAG with scheduling, monitoring, retries
# ============================================================
if AIRFLOW_AVAILABLE:
    default_args_adv = {
        "owner"             : "power_grid_admin",
        "depends_on_past"   : False,
        "retries"           : 3,
        "retry_delay"       : timedelta(minutes=5),
        "retry_exponential_backoff": True,
        "start_date"        : datetime(2026, 4, 1),
        "email_on_failure"  : False,
        "email_on_retry"    : False,
    }

    with DAG(
        dag_id="task23_power_grid_etl_advanced",
        default_args=default_args_adv,
        description="Advanced ETL DAG - Power Grid with monitoring & alerts",
        schedule_interval="0 */6 * * *",   # Every 6 hours
        catchup=False,
        max_active_runs=1,
        tags=["power_grid", "etl", "advanced", "monitoring"],
    ) as dag_advanced:

        t_extract   = PythonOperator(task_id="extract",        python_callable=extract_power_data)
        t_validate  = PythonOperator(task_id="validate",       python_callable=validate_data)
        t_transform = PythonOperator(task_id="transform",      python_callable=transform_data)
        t_load      = PythonOperator(task_id="load_to_db",     python_callable=load_to_db)
        t_report    = PythonOperator(task_id="generate_report",python_callable=generate_report)
        t_alert     = PythonOperator(task_id="send_alert",     python_callable=send_alert)

        # DAG dependency chain
        t_extract >> t_validate >> t_transform >> t_load >> [t_report, t_alert]

# ============================================================
# SIMULATION: Run pipeline without Airflow
# ============================================================
if __name__ == "__main__":
    print("=" * 60)
    print("  Tasks 22–23: Airflow ETL Pipeline Simulation")
    print("=" * 60)

    steps = [
        ("1. Extract",          extract_power_data),
        ("2. Validate",         validate_data),
        ("3. Transform",        transform_data),
        ("4. Load to DB",       load_to_db),
        ("5. Generate Report",  generate_report),
        ("6. Send Alert",       send_alert),
    ]

    total_start = time.time()
    for name, fn in steps:
        print(f"\n[Running] {name}...")
        step_start = time.time()
        try:
            result = fn()
            elapsed = round((time.time() - step_start) * 1000, 1)
            print(f"  ✅ {name} complete in {elapsed}ms" + (f" → {result} rows" if result else ""))
        except Exception as e:
            print(f"  ❌ {name} FAILED: {e}")

    total_time = round(time.time() - total_start, 2)
    print(f"\n  Pipeline completed in {total_time}s")

    if AIRFLOW_AVAILABLE:
        print("\n  Airflow DAGs registered:")
        print("    - task22_power_grid_etl_basic     (daily)")
        print("    - task23_power_grid_etl_advanced  (every 6 hours, retries=3)")
    else:
        print("\n  ℹ  Airflow not installed. Pipeline ran as standalone Python.")
        print("     To use Airflow: pip install apache-airflow")
        print("     Then copy this file to ~/airflow/dags/")

    print("\n✅ Tasks 22–23: Airflow DAG Complete.")
