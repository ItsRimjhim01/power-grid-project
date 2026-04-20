# ⚡ Power Grid Analytics Dashboard
### Capstone Project — Data Engineering Program

---

## 📌 Project Overview

A full-stack, end-to-end Data Engineering project that monitors electricity consumption across four regions (North, South, East, West) in real time. The project demonstrates all 30 Data Engineering tasks — from Linux setup and SQL to Spark, Kafka streaming, cloud architecture, and a live React dashboard.

---

## 🏗️ Project Architecture

```
[CSV / IoT Sensors]
      ↓
[ETL Pipeline (Python)]
      ↓
[MySQL / SQLite Database]
      ↓
[Node.js REST API (Express)]
      ↓
[React Frontend Dashboard]
```

---

## 📁 Folder Structure

```
power-grid-project/
├── backend/                    # Node.js Express API
│   ├── server.js               # REST API (6 endpoints)
│   └── package.json
├── frontend/                   # React Dashboard
│   └── src/
│       └── App.js              # Full analytics UI
├── data/
│   ├── power_data.csv          # Raw data
│   ├── raw/                    # Ingested raw files
│   ├── processed/              # Cleaned & transformed
│   └── warehouse/              # Star Schema DBs
├── scripts/
│   ├── etl.py                  # Main ETL pipeline
│   ├── generate_data.py        # Data generator
│   └── tasks/                  # All 30 task scripts
│       ├── task01_linux_filesystem.sh
│       ├── task02_networking.py
│       ├── task03_python_basics.py
│       ├── task04_advanced_python.py
│       ├── task05_pandas_numpy.py
│       ├── task09_data_warehouse.py
│       ├── task10_etl_vs_elt.py
│       ├── task11_data_ingestion.py
│       ├── task24_27_cloud.py
│       ├── task28_lakehouse.py
│       ├── task29_data_quality.py
│       └── task30_final_pipeline.py   ← Master pipeline runner
├── sql/
│   └── queries.sql             # Tasks 6, 7, 8 SQL
├── spark/
│   └── task14_17_spark_simulation.py
├── streaming/
│   └── task18_21_streaming_kafka.py
├── hadoop/
│   └── task12_13_hdfs_simulation.py
├── airflow_dags/
│   └── task22_23_airflow_dag.py
└── logs/                       # Pipeline & alert logs
```

---

## 🚀 Quick Start

### 1. Generate Data
```bash
python3 scripts/generate_data.py
```

### 2. Run ETL
```bash
python3 scripts/etl.py
```

### 3. Start Backend API
```bash
cd backend
npm install
node server.js
```

### 4. Start Frontend
```bash
cd frontend
npm install
npm start
```
Visit: http://localhost:3000

---

## 🔌 API Endpoints

| Endpoint        | Method | Description                        |
|-----------------|--------|------------------------------------|
| `/health`       | GET    | Server health check                |
| `/data`         | GET    | Latest 50 records (filter by region)|
| `/region`       | GET    | Region-wise average consumption    |
| `/summary`      | GET    | Total, avg, max, min stats         |
| `/alerts`       | GET    | Critical consumption alerts        |
| `/trend/hourly` | GET    | Hourly average trend               |

---

## ⚙️ Environment Variables (Backend)

```bash
export DB_HOST=localhost
export DB_USER=root
export DB_PASS=your_secure_password
export DB_NAME=powergrid
export PORT=5000
```

---

## 📋 30 Tasks Coverage

| # | Task | File |
|---|------|------|
| 1 | Linux + File System | `scripts/tasks/task01_linux_filesystem.sh` |
| 2 | Networking | `scripts/tasks/task02_networking.py` |
| 3 | Python Basics | `scripts/tasks/task03_python_basics.py` |
| 4 | Advanced Python | `scripts/tasks/task04_advanced_python.py` |
| 5 | Pandas + NumPy | `scripts/tasks/task05_pandas_numpy.py` |
| 6 | SQL Basics | `sql/queries.sql` |
| 7 | Advanced SQL | `sql/queries.sql` |
| 8 | DB Concepts (OLTP/OLAP) | `sql/queries.sql` |
| 9 | Data Warehousing | `scripts/tasks/task09_data_warehouse.py` |
| 10 | ETL vs ELT | `scripts/tasks/task10_etl_vs_elt.py` |
| 11 | Data Ingestion | `scripts/tasks/task11_data_ingestion.py` |
| 12 | Hadoop | `hadoop/task12_13_hdfs_simulation.py` |
| 13 | HDFS Architecture | `hadoop/task12_13_hdfs_simulation.py` |
| 14 | Spark Basics | `spark/task14_17_spark_simulation.py` |
| 15 | Spark DataFrames | `spark/task14_17_spark_simulation.py` |
| 16 | Spark SQL | `spark/task14_17_spark_simulation.py` |
| 17 | PySpark Advanced | `spark/task14_17_spark_simulation.py` |
| 18 | Streaming Concepts | `streaming/task18_21_streaming_kafka.py` |
| 19 | Kafka Basics | `streaming/task18_21_streaming_kafka.py` |
| 20 | Kafka Advanced | `streaming/task18_21_streaming_kafka.py` |
| 21 | Structured Streaming | `streaming/task18_21_streaming_kafka.py` |
| 22 | Airflow Basics | `airflow_dags/task22_23_airflow_dag.py` |
| 23 | Airflow Advanced | `airflow_dags/task22_23_airflow_dag.py` |
| 24 | Cloud Basics | `scripts/tasks/task24_27_cloud.py` |
| 25 | Cloud Storage | `scripts/tasks/task24_27_cloud.py` |
| 26 | Cloud Compute | `scripts/tasks/task24_27_cloud.py` |
| 27 | Cloud Data Warehouse | `scripts/tasks/task24_27_cloud.py` |
| 28 | Lakehouse | `scripts/tasks/task28_lakehouse.py` |
| 29 | Data Quality | `scripts/tasks/task29_data_quality.py` |
| 30 | Final Pipeline | `scripts/tasks/task30_final_pipeline.py` |

---

## 🛠️ Tech Stack

- **Frontend**: React.js, Recharts, Axios
- **Backend**: Node.js, Express.js, CORS
- **Database**: MySQL (prod), SQLite (dev/fallback)
- **ETL**: Python, Pandas, NumPy
- **Storage**: Parquet (Data Lake), SQLite (DW simulation)
- **Orchestration**: Apache Airflow (DAG definitions)
- **Streaming**: Kafka simulation (Python threading)
- **Big Data**: Apache Spark / PySpark (simulation)
- **Cloud**: AWS/GCP/Azure architecture diagrams
- **Version Control**: Git + GitHub

---

## 👨‍💻 Student Details

- **Name**: [Your Name]
- **Roll Number**: [Your Roll Number]
- **Batch/Program**: Data Engineering — 2026

---

*Capstone Deadline: April 21, 2026 | Evaluation Test: April 23, 2026*
