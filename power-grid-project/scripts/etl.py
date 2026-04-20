"""
ETL Script - Power Grid Analytics Dashboard
Description: Complete ETL pipeline: Extract from CSV → Transform → Load to MySQL/SQLite
             Also supports ELT mode.
"""

import pandas as pd
import os
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Try MySQL; fall back to SQLite for portability
try:
    import mysql.connector
    USE_MYSQL = True
except ImportError:
    USE_MYSQL = False

import sqlite3

DB_CONFIG = {
    "host"    : os.getenv("DB_HOST", "localhost"),
    "user"    : os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASS", ""),     # Set via env var — never hardcode
    "database": os.getenv("DB_NAME", "powergrid"),
}

def extract(path="data/power_data.csv"):
    logging.info(f"[EXTRACT] Reading {path}")
    df = pd.read_csv(path)
    logging.info(f"[EXTRACT] {len(df)} rows loaded")
    return df

def transform(df):
    logging.info("[TRANSFORM] Cleaning and enriching data...")
    df = df.copy()
    df["timestamp"]   = pd.to_datetime(df["timestamp"])
    df.dropna(inplace=True)
    df = df[df["consumption"] > 0]
    df["hour"]        = df["timestamp"].dt.hour
    df["is_peak"]     = df["hour"].between(8, 20).astype(int)
    df["load_level"]  = pd.cut(df["consumption"],
                               bins=[0,200,300,400,float("inf")],
                               labels=["Low","Normal","High","Critical"])
    df["timestamp"]   = df["timestamp"].astype(str)
    logging.info(f"[TRANSFORM] Done. {len(df)} rows after cleaning.")
    return df

def load(df):
    logging.info("[LOAD] Writing to database...")
    os.makedirs("data/processed", exist_ok=True)

    mysql_loaded = False
    if USE_MYSQL:
        try:
            conn = mysql.connector.connect(**DB_CONFIG)
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS power_usage (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    timestamp DATETIME, region VARCHAR(20),
                    consumption FLOAT, hour INT, is_peak INT, load_level VARCHAR(10)
                )
            """)
            for _, row in df.iterrows():
                cursor.execute(
                    "INSERT INTO power_usage (timestamp,region,consumption,hour,is_peak,load_level) VALUES (%s,%s,%s,%s,%s,%s)",
                    (row["timestamp"], row["region"], row["consumption"], int(row["hour"]), int(row["is_peak"]), str(row["load_level"]))
                )
            conn.commit()
            cursor.close()
            conn.close()
            logging.info(f"[LOAD] {len(df)} rows loaded to MySQL.")
            mysql_loaded = True
        except Exception as e:
            logging.warning(f"[LOAD] MySQL failed ({e}). Falling back to SQLite.")

    if not mysql_loaded:
        conn = sqlite3.connect("data/processed/powergrid_ops.db")
        df.to_sql("power_usage", conn, if_exists="replace", index=False)
        count = conn.execute("SELECT COUNT(*) FROM power_usage").fetchone()[0]
        conn.close()
        logging.info(f"[LOAD] {count} rows loaded to SQLite.")
        print(f"  ✅ Loaded {count} rows → data/processed/powergrid_ops.db")

if __name__ == "__main__":
    df_raw  = extract()
    df_clean = transform(df_raw)
    load(df_clean)
    print("ETL pipeline completed successfully.")
