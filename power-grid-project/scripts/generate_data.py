"""
Data Generator - Power Grid Analytics Dashboard
Description: Generate realistic power grid CSV data with multiple attributes.
             Supports generating 100 rows (default) or large datasets.
"""

import pandas as pd
import numpy as np
import random
import os
import sys
from datetime import datetime, timedelta

print("Script started")

os.makedirs("data", exist_ok=True)

# Configuration
N = int(sys.argv[1]) if len(sys.argv) > 1 else 100
regions = ["North", "South", "East", "West"]
start   = datetime.now()

np.random.seed(42)
rows = []

for i in range(N):
    ts         = start + timedelta(minutes=i)
    region     = random.choice(regions)
    hour       = ts.hour

    # Realistic consumption: higher during peak hours (8–20)
    base = 350 if 8 <= hour <= 20 else 180
    consumption = round(max(50, base + np.random.normal(0, 60)), 2)

    voltage     = round(np.random.uniform(220, 240), 1)
    frequency   = round(np.random.uniform(49.5, 50.5), 3)

    rows.append({
        "timestamp"  : ts,
        "region"     : region,
        "consumption": consumption,
        "voltage"    : voltage,
        "frequency"  : frequency,
    })

df = pd.DataFrame(rows)
df.to_csv("data/power_data.csv", index=False)
print(f"Dataset created successfully! ({N} rows → data/power_data.csv)")
