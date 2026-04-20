"""
Tasks 24–27: Cloud Computing Tasks
Project: Power Grid Analytics Dashboard
Description:
  Task 24 - Compare AWS, GCP, Azure services
  Task 25 - Cloud Storage (S3/GCS simulation)
  Task 26 - Cloud Compute (EC2 deployment simulation)
  Task 27 - Cloud Data Warehouse (BigQuery/Redshift simulation)
"""

import os
import json
import time
import sqlite3
import pandas as pd
import shutil
from datetime import datetime

print("=" * 60)
print("  Tasks 24–27: Cloud Computing - Power Grid Analytics")
print("=" * 60)

# ============================================================
# TASK 24: Compare AWS vs GCP vs Azure
# ============================================================
print("\n" + "="*50)
print("  TASK 24: Cloud Platform Comparison")
print("="*50)

cloud_comparison = {
    "Service Category"    : ["Object Storage",   "Compute",      "Managed Kafka",     "Data Warehouse",   "Stream Processing", "Orchestration",     "ML Platform"],
    "AWS"                 : ["S3",               "EC2",          "MSK",               "Redshift",         "Kinesis",           "Step Functions",    "SageMaker"],
    "GCP"                 : ["GCS",              "Compute Engine","Pub/Sub",           "BigQuery",         "Dataflow",          "Cloud Composer",    "Vertex AI"],
    "Azure"               : ["Blob Storage",     "Azure VM",     "Event Hubs",        "Synapse Analytics","Stream Analytics",  "Azure Data Factory","Azure ML"],
    "Used in This Project": ["CSV / Data Lake",  "Backend API",  "Kafka Streaming",   "DW Queries",       "Real-time alerts",  "Airflow DAGs",      "Anomaly detect"],
}

print(f"\n  {'Category':<22} {'AWS':<18} {'GCP':<20} {'Azure':<22}")
print("  " + "-"*82)
for i in range(7):
    cat  = cloud_comparison["Service Category"][i]
    aws  = cloud_comparison["AWS"][i]
    gcp  = cloud_comparison["GCP"][i]
    az   = cloud_comparison["Azure"][i]
    used = cloud_comparison["Used in This Project"][i]
    print(f"  {cat:<22} {aws:<18} {gcp:<20} {az:<22}")

print("""
  Recommended for Power Grid Project:
  ┌─────────────────────────────────────────────────────┐
  │  Storage    → AWS S3 or GCS (cheap, scalable)       │
  │  Compute    → AWS EC2 (Node.js + Python backend)    │
  │  Streaming  → AWS Kinesis or Confluent Kafka         │
  │  Warehouse  → BigQuery (serverless, fast SQL)        │
  │  Orchestrate→ Airflow on Cloud Composer (GCP)        │
  └─────────────────────────────────────────────────────┘
""")

# ============================================================
# TASK 25: Cloud Storage Simulation (S3/GCS)
# ============================================================
print("="*50)
print("  TASK 25: Cloud Storage Simulation (S3/GCS)")
print("="*50)

# Simulate an S3-like bucket using local directories
BUCKET_ROOT = "cloud_sim/s3_bucket"
BUCKET_NAME = "power-grid-data-lake"
BUCKET_PATH = f"{BUCKET_ROOT}/{BUCKET_NAME}"

os.makedirs(f"{BUCKET_PATH}/raw/", exist_ok=True)
os.makedirs(f"{BUCKET_PATH}/processed/", exist_ok=True)
os.makedirs(f"{BUCKET_PATH}/archive/", exist_ok=True)

bucket_index = {}  # simulate S3 object metadata

def s3_upload(local_path, s3_key):
    """Simulate uploading a file to S3."""
    dest = f"{BUCKET_PATH}/{s3_key}"
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    shutil.copy2(local_path, dest)
    size = os.path.getsize(dest)
    bucket_index[s3_key] = {
        "key": s3_key, "size_bytes": size,
        "last_modified": str(datetime.now()), "etag": f"etag-{hash(s3_key)}"
    }
    print(f"  [s3_upload] s3://{BUCKET_NAME}/{s3_key}  ({size} bytes) ✅")

def s3_download(s3_key, local_dest):
    """Simulate downloading from S3."""
    src = f"{BUCKET_PATH}/{s3_key}"
    if os.path.exists(src):
        shutil.copy2(src, local_dest)
        print(f"  [s3_download] s3://{BUCKET_NAME}/{s3_key} → {local_dest} ✅")
    else:
        print(f"  [s3_download] ERROR: Key not found: {s3_key}")

def s3_list(prefix=""):
    """Simulate listing S3 objects."""
    return [k for k in bucket_index if k.startswith(prefix)]

# Upload files
print()
src = "data/power_data.csv"
if os.path.exists(src):
    s3_upload(src, "raw/power_data_2026_04.csv")
    s3_upload(src, "processed/power_clean_2026_04.csv")

# List
print(f"\n  [s3_list] s3://{BUCKET_NAME}/")
for key in s3_list():
    meta = bucket_index[key]
    print(f"    {key:<45} {meta['size_bytes']} bytes | {meta['last_modified'][:19]}")

# Download
s3_download("raw/power_data_2026_04.csv", "data/downloaded_from_s3.csv")

# ============================================================
# TASK 26: Cloud Compute - EC2 Deployment Simulation
# ============================================================
print("\n" + "="*50)
print("  TASK 26: Cloud Compute - EC2 Deployment Simulation")
print("="*50)

print("""
  EC2 Deployment Plan for Power Grid Backend:
  ============================================
  Instance Type : t3.medium (2 vCPU, 4GB RAM)
  OS            : Ubuntu 22.04 LTS
  Region        : ap-south-1 (Mumbai)
  Security Group: Port 5000 (API), Port 22 (SSH), Port 3306 (MySQL internal)

  Deployment Steps (simulated):
""")

ec2_steps = [
    ("Launch EC2 Instance",        "aws ec2 run-instances --image-id ami-0c55b159cbfafe1f0 --instance-type t3.medium"),
    ("SSH into instance",           "ssh -i power-grid-key.pem ubuntu@<EC2-PUBLIC-IP>"),
    ("Install Node.js",             "curl -sL https://deb.nodesource.com/setup_18.x | sudo bash - && sudo apt install nodejs"),
    ("Install MySQL",               "sudo apt install mysql-server -y && sudo mysql_secure_installation"),
    ("Upload project files",        "scp -i power-grid-key.pem -r ./backend ubuntu@<IP>:/home/ubuntu/power-grid/"),
    ("Install dependencies",        "cd /home/ubuntu/power-grid/backend && npm install"),
    ("Set environment variables",   "export DB_PASS='<secure_password>' DB_HOST='localhost' DB_NAME='powergrid'"),
    ("Start server with PM2",       "npm install -g pm2 && pm2 start server.js --name power-grid-api"),
    ("Enable auto-restart",         "pm2 startup && pm2 save"),
    ("Configure Nginx reverse proxy","sudo apt install nginx && sudo nano /etc/nginx/sites-available/power-grid"),
]

for i, (step, cmd) in enumerate(ec2_steps, 1):
    print(f"  {i:02d}. {step}")
    print(f"      $ {cmd}\n")

# Simulate server health check
print("  [Health Check Simulation]")
health = {"status": "OK", "message": "Server is running", "time": str(datetime.now()), "instance": "i-0abc1234567890ef0", "region": "ap-south-1"}
print(f"  GET /health → {json.dumps(health, indent=6)}")

# ============================================================
# TASK 27: Cloud Data Warehouse (BigQuery/Redshift simulation)
# ============================================================
print("\n" + "="*50)
print("  TASK 27: Cloud Data Warehouse (BigQuery Simulation)")
print("="*50)

os.makedirs("data/warehouse", exist_ok=True)
DW_PATH = "data/warehouse/bigquery_sim.db"
conn_bq = sqlite3.connect(DW_PATH)

# Load data
df = pd.read_csv("data/power_data.csv")
df["timestamp"] = pd.to_datetime(df["timestamp"]).astype(str)
df.to_sql("power_usage", conn_bq, if_exists="replace", index=False)

print(f"\n  [BigQuery] Dataset loaded: {len(df):,} rows")

bq_queries = {
    "Q1 - Partitioned query (by region)":
        "SELECT region, ROUND(AVG(consumption),2) AS avg_kwh, COUNT(*) AS records FROM power_usage GROUP BY region",

    "Q2 - Time-series trend (simulated daily)":
        "SELECT substr(timestamp,1,10) AS date, ROUND(AVG(consumption),2) AS avg_kwh FROM power_usage GROUP BY substr(timestamp,1,10) ORDER BY date",

    "Q3 - Top consumption (LIMIT 5)":
        "SELECT timestamp, region, ROUND(consumption,2) AS consumption FROM power_usage ORDER BY consumption DESC LIMIT 5",

    "Q4 - Consumption distribution":
        """SELECT
            CASE WHEN consumption<200 THEN 'Low' WHEN consumption<300 THEN 'Normal'
                 WHEN consumption<400 THEN 'High' ELSE 'Critical' END AS load_level,
            COUNT(*) AS count
           FROM power_usage GROUP BY load_level"""
}

for qname, qsql in bq_queries.items():
    print(f"\n  [{qname}]")
    start = time.time()
    result = pd.read_sql(qsql, conn_bq)
    elapsed = round((time.time()-start)*1000, 2)
    print(result.to_string(index=False))
    print(f"  Query time: {elapsed} ms  (in BigQuery this scales to PB-level data)")

conn_bq.close()
print(f"\n  BigQuery simulation DB → {DW_PATH}")
print("\n✅ Tasks 24–27: Cloud Computing Tasks Complete.")
