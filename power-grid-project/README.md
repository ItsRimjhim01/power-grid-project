# ⚡ Power Grid Analytics Dashboard

### End-to-End Data Engineering Capstone Project

---

## 📌 Overview

The **Power Grid Analytics Dashboard** is a full-stack, end-to-end data engineering project designed to monitor and analyze electricity consumption across multiple regions (North, South, East, West) in near real-time.

This project demonstrates the complete data engineering lifecycle — from data ingestion and transformation to storage, processing, and visualization — covering **30 industry-relevant tasks**.

---

## 🎯 Key Highlights

* ⚡ Real-time energy consumption monitoring
* 📊 Interactive analytics dashboard with dynamic charts
* 🔄 End-to-end ETL pipeline (raw → processed → warehouse)
* 🌐 REST API with multiple analytical endpoints
* ☁️ Simulated big data ecosystem (Spark, Kafka, Hadoop, Airflow)
* 📦 Modular and scalable project structure

---

## 🏗️ Architecture

```
[CSV / IoT Sensors]
        ↓
[Python ETL Pipeline]
        ↓
[MySQL / SQLite]
        ↓
[Node.js REST API]
        ↓
[React Dashboard]
```

---

## 📁 Project Structure

```
power-grid-project/
├── backend/              # Node.js API
├── frontend/             # React dashboard
├── data/                 # Raw, processed, warehouse data
├── scripts/              # ETL + task implementations
├── sql/                  # SQL queries
├── spark/                # Spark simulations
├── streaming/            # Kafka simulations
├── hadoop/               # HDFS simulations
├── airflow_dags/         # Airflow DAGs
└── logs/                 # Logs and alerts
```

---

## 🚀 Getting Started

### 1️⃣ Generate Data

```bash
python3 scripts/generate_data.py
```

### 2️⃣ Run ETL Pipeline

```bash
python3 scripts/etl.py
```

### 3️⃣ Start Backend

```bash
cd backend
npm install
node server.js
```

### 4️⃣ Start Frontend

```bash
cd frontend
npm install
npm start
```

📍 Open: http://localhost:3000

---

## 🔌 API Endpoints

| Endpoint      | Method | Description          |
| ------------- | ------ | -------------------- |
| /health       | GET    | Server health check  |
| /data         | GET    | Latest records       |
| /region       | GET    | Region-wise averages |
| /summary      | GET    | Aggregate statistics |
| /alerts       | GET    | Critical alerts      |
| /trend/hourly | GET    | Hourly trends        |

---

## ⚙️ Environment Variables

```bash
DB_HOST=localhost
DB_USER=root
DB_PASS=your_password
DB_NAME=powergrid
PORT=5000
```

---

## 📊 Tech Stack

### 💻 Frontend

* React.js
* Recharts
* Axios

### ⚙️ Backend

* Node.js
* Express.js

### 🗄️ Data Layer

* MySQL
* SQLite
* Parquet

### 🔄 Data Processing

* Python
* Pandas
* NumPy

### ☁️ Big Data & Tools

* Apache Spark (simulation)
* Kafka (simulation)
* Hadoop (simulation)
* Apache Airflow

### 🔧 Dev Tools

* Git & GitHub

---

## 📋 Data Engineering Coverage

This project implements **30 core data engineering concepts**, including:

* Linux & Networking
* Python & Data Processing
* SQL & Data Warehousing
* ETL Pipelines
* Hadoop & HDFS
* Spark & Distributed Processing
* Kafka & Streaming
* Airflow Orchestration
* Cloud & Lakehouse Concepts
* Data Quality & Monitoring

---

## 📈 Features

* 📊 Region-wise consumption analysis
* 📉 Trend visualization (hourly patterns)
* 🚨 Alert system for abnormal usage
* 📦 Structured data pipeline
* 🔍 Clean and modular codebase

---

## 🧑‍💻 Author

* **Name**: Rimjhim Kumari
* **Program**: Data Engineering (2026)

---

## ⭐ Project Value

This project demonstrates:

* End-to-end data engineering skills
* Real-world system design thinking
* Full-stack development integration
* Hands-on experience with modern data tools

---

## 📌 Future Enhancements

* Real-time Kafka integration (production-level)
* Cloud deployment (AWS/GCP)
* Authentication & user roles
* Advanced ML-based forecasting

---

## 📄 License

This project is for academic and learning purposes.
