"""
Task 11: Data Ingestion Task
Project: Power Grid Analytics Dashboard
Description: Batch ingestion pipeline from CSV to SQLite database.
"""

import pandas as pd
import sqlite3
import os
import time
import logging
from datetime import datetime

# Setup logging
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    filename="logs/ingestion.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

def log(msg):
    print(f"  [{datetime.now().strftime('%H:%M:%S')}] {msg}")
    logging.info(msg)

print("=" * 60)
print("  Task 11: Batch Data Ingestion Pipeline (CSV → SQLite)")
print("=" * 60)

SOURCE_CSV = "data/power_data.csv"
DB_PATH    = "data/processed/powergrid_ingest.db"
BATCH_SIZE = 20  # Process 20 rows at a time

# -------------------------------------------------------
# Step 1: Validate source file
# -------------------------------------------------------
print()
log("Step 1: Validating source CSV file...")

if not os.path.exists(SOURCE_CSV):
    log("ERROR: Source CSV not found.")
    raise FileNotFoundError(f"{SOURCE_CSV} not found")

df_source = pd.read_csv(SOURCE_CSV)
log(f"Source file loaded: {len(df_source)} rows, {df_source.shape[1]} columns")
log(f"Columns: {list(df_source.columns)}")

# -------------------------------------------------------
# Step 2: Create target database schema
# -------------------------------------------------------
log("Step 2: Setting up SQLite database schema...")
conn = sqlite3.connect(DB_PATH)
cur  = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS power_usage (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp   TEXT NOT NULL,
    region      TEXT NOT NULL,
    consumption REAL NOT NULL,
    ingested_at TEXT DEFAULT (datetime('now'))
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS ingestion_log (
    log_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_num   INTEGER,
    rows_loaded INTEGER,
    status      TEXT,
    logged_at   TEXT DEFAULT (datetime('now'))
)
""")
conn.commit()
log("Database schema created/verified.")

# -------------------------------------------------------
# Step 3: Batch ingestion
# -------------------------------------------------------
log(f"Step 3: Starting batch ingestion (batch size={BATCH_SIZE})...")
start = time.time()
total_rows  = 0
failed_rows = 0
batch_num   = 0

for i in range(0, len(df_source), BATCH_SIZE):
    batch     = df_source.iloc[i:i + BATCH_SIZE].copy()
    batch_num += 1

    # Data cleaning per batch
    batch.dropna(inplace=True)
    batch = batch[batch["consumption"] > 0]
    batch["timestamp"] = pd.to_datetime(batch["timestamp"]).astype(str)

    try:
        for _, row in batch.iterrows():
            cur.execute(
                "INSERT INTO power_usage (timestamp, region, consumption) VALUES (?,?,?)",
                (row["timestamp"], row["region"], row["consumption"])
            )
        conn.commit()
        total_rows += len(batch)

        cur.execute(
            "INSERT INTO ingestion_log (batch_num, rows_loaded, status) VALUES (?,?,?)",
            (batch_num, len(batch), "SUCCESS")
        )
        conn.commit()
        log(f"Batch {batch_num}: {len(batch)} rows ingested ✅")

    except Exception as e:
        conn.rollback()
        failed_rows += len(batch)
        cur.execute(
            "INSERT INTO ingestion_log (batch_num, rows_loaded, status) VALUES (?,?,?)",
            (batch_num, 0, f"FAILED: {e}")
        )
        conn.commit()
        log(f"Batch {batch_num}: FAILED ❌ → {e}")

elapsed = round((time.time() - start) * 1000, 2)

# -------------------------------------------------------
# Step 4: Verification
# -------------------------------------------------------
log("Step 4: Verifying ingested data...")
count = cur.execute("SELECT COUNT(*) FROM power_usage").fetchone()[0]
sample = pd.read_sql("SELECT * FROM power_usage LIMIT 5", conn)

print()
print("  Ingestion Summary:")
print(f"  ├── Total rows ingested : {total_rows}")
print(f"  ├── Failed rows         : {failed_rows}")
print(f"  ├── DB record count     : {count}")
print(f"  ├── Total batches       : {batch_num}")
print(f"  └── Time taken          : {elapsed} ms")
print()
print("  Sample from DB:")
print(sample.to_string(index=False))

conn.close()
log(f"Ingestion complete. {total_rows} rows in {elapsed}ms.")
print(f"\n✅ Task 11: Batch Ingestion Pipeline Complete. DB → {DB_PATH}")
