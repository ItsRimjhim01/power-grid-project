"""
Task 9: Data Warehousing Task
Project: Power Grid Analytics Dashboard
Description: Design a Star Schema for Power Grid Analytics.
             Includes fact table and dimension tables.
"""

# =========================================================
# STAR SCHEMA DESIGN - Power Grid Analytics Data Warehouse
# =========================================================
#
#  FACT TABLE:
#    fact_power_consumption
#    - consumption_id (PK)
#    - time_key (FK → dim_time)
#    - region_key (FK → dim_region)
#    - grid_key (FK → dim_grid)
#    - consumption_kwh
#    - voltage_v
#    - frequency_hz
#    - load_factor
#
#  DIMENSION TABLES:
#    dim_time
#    - time_key (PK)
#    - full_datetime
#    - hour, day, month, year
#    - day_of_week, is_weekend
#    - is_peak_hour
#
#    dim_region
#    - region_key (PK)
#    - region_name (North/South/East/West)
#    - zone
#    - state
#    - capacity_mw
#
#    dim_grid
#    - grid_key (PK)
#    - grid_id
#    - grid_type (Transmission/Distribution)
#    - substation_name
#
# =========================================================

import pandas as pd
import numpy as np
import sqlite3
import os
from datetime import datetime, timedelta

print("=" * 60)
print("  Task 9: Data Warehousing - Star Schema Design")
print("=" * 60)

os.makedirs("data/warehouse", exist_ok=True)
DB_PATH = "data/warehouse/power_grid_dw.db"
conn = sqlite3.connect(DB_PATH)
cur  = conn.cursor()

# -------------------------------------------------------
# Create Dimension Tables
# -------------------------------------------------------
cur.executescript("""
DROP TABLE IF EXISTS fact_power_consumption;
DROP TABLE IF EXISTS dim_time;
DROP TABLE IF EXISTS dim_region;
DROP TABLE IF EXISTS dim_grid;

CREATE TABLE dim_time (
    time_key     INTEGER PRIMARY KEY AUTOINCREMENT,
    full_datetime TEXT NOT NULL,
    hour         INTEGER,
    day          INTEGER,
    month        INTEGER,
    year         INTEGER,
    day_of_week  TEXT,
    is_weekend   INTEGER,
    is_peak_hour INTEGER
);

CREATE TABLE dim_region (
    region_key  INTEGER PRIMARY KEY AUTOINCREMENT,
    region_name TEXT NOT NULL,
    zone        TEXT,
    state       TEXT,
    capacity_mw REAL
);

CREATE TABLE dim_grid (
    grid_key        INTEGER PRIMARY KEY AUTOINCREMENT,
    grid_id         TEXT NOT NULL,
    grid_type       TEXT,
    substation_name TEXT
);

CREATE TABLE fact_power_consumption (
    consumption_id INTEGER PRIMARY KEY AUTOINCREMENT,
    time_key       INTEGER REFERENCES dim_time(time_key),
    region_key     INTEGER REFERENCES dim_region(region_key),
    grid_key       INTEGER REFERENCES dim_grid(grid_key),
    consumption_kwh REAL,
    voltage_v       REAL,
    frequency_hz    REAL,
    load_factor     REAL
);
""")

# -------------------------------------------------------
# Populate Dimension Tables
# -------------------------------------------------------
# dim_region
regions_data = [
    ("North", "Zone-A", "Delhi",     1500.0),
    ("South", "Zone-B", "Chennai",   1200.0),
    ("East",  "Zone-C", "Kolkata",    900.0),
    ("West",  "Zone-D", "Mumbai",    1100.0),
]
cur.executemany("INSERT INTO dim_region (region_name, zone, state, capacity_mw) VALUES (?,?,?,?)", regions_data)

# dim_grid
grids_data = [
    ("G001", "Transmission",  "Sub-Alpha"),
    ("G002", "Distribution",  "Sub-Beta"),
    ("G003", "Transmission",  "Sub-Gamma"),
    ("G004", "Distribution",  "Sub-Delta"),
]
cur.executemany("INSERT INTO dim_grid (grid_id, grid_type, substation_name) VALUES (?,?,?)", grids_data)

# dim_time + fact_power_consumption
np.random.seed(42)
base_time = datetime(2026, 4, 1, 0, 0, 0)
N = 200

for i in range(N):
    dt = base_time + timedelta(hours=i)
    hour        = dt.hour
    is_weekend  = 1 if dt.weekday() >= 5 else 0
    is_peak     = 1 if 8 <= hour <= 20 else 0
    day_name    = dt.strftime("%A")

    cur.execute("""
        INSERT INTO dim_time (full_datetime, hour, day, month, year, day_of_week, is_weekend, is_peak_hour)
        VALUES (?,?,?,?,?,?,?,?)
    """, (str(dt), hour, dt.day, dt.month, dt.year, day_name, is_weekend, is_peak))
    time_key = cur.lastrowid

    region_key = np.random.randint(1, 5)
    grid_key   = np.random.randint(1, 5)
    consumption = round(np.random.uniform(100, 500), 2)
    voltage     = round(np.random.uniform(220, 240), 1)
    frequency   = round(np.random.uniform(49.5, 50.5), 3)
    capacity    = regions_data[region_key - 1][3]
    load_factor = round(consumption / capacity, 4)

    cur.execute("""
        INSERT INTO fact_power_consumption
        (time_key, region_key, grid_key, consumption_kwh, voltage_v, frequency_hz, load_factor)
        VALUES (?,?,?,?,?,?,?)
    """, (time_key, region_key, grid_key, consumption, voltage, frequency, load_factor))

conn.commit()

# -------------------------------------------------------
# Demo Analytical Queries on Star Schema
# -------------------------------------------------------
print("\n[+] Star Schema Query: Region-wise Monthly Consumption")
query = """
SELECT dr.region_name, dt.month, dt.year,
    ROUND(AVG(f.consumption_kwh), 2) AS avg_kwh,
    ROUND(SUM(f.consumption_kwh), 2) AS total_kwh,
    ROUND(AVG(f.load_factor)*100, 2) AS avg_load_pct
FROM fact_power_consumption f
JOIN dim_region dr ON f.region_key = dr.region_key
JOIN dim_time   dt ON f.time_key   = dt.time_key
GROUP BY dr.region_name, dt.year, dt.month
ORDER BY dr.region_name
"""
df = pd.read_sql_query(query, conn)
print(df.to_string(index=False))

print("\n[+] Peak vs Off-Peak analysis:")
query2 = """
SELECT dt.is_peak_hour,
    ROUND(AVG(f.consumption_kwh), 2) AS avg_kwh,
    COUNT(*) AS records
FROM fact_power_consumption f
JOIN dim_time dt ON f.time_key = dt.time_key
GROUP BY dt.is_peak_hour
"""
df2 = pd.read_sql_query(query2, conn)
df2["period"] = df2["is_peak_hour"].map({1: "Peak (8am–8pm)", 0: "Off-Peak"})
print(df2[["period", "avg_kwh", "records"]].to_string(index=False))

conn.close()
print(f"\n  Star Schema DB saved → {DB_PATH}")
print("\n✅ Task 9: Data Warehousing (Star Schema) Complete.")
