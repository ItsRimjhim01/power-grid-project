const express = require("express");
const cors    = require("cors");
const path    = require("path");
const fs      = require("fs");

// Try MySQL; fall back to SQLite for portability
let db, useMySQL = false;

try {
  const mysql = require("mysql2");
  db = mysql.createConnection({
    host    : process.env.DB_HOST || "localhost",
    user    : process.env.DB_USER || "root",
    password: process.env.DB_PASS || "",        // Use env variable — never hardcode!
    database: process.env.DB_NAME || "powergrid"
  });
  db.connect(err => {
    if (err) {
      console.warn("[WARN] MySQL connection failed. Falling back to SQLite.");
      initSQLite();
    } else {
      useMySQL = true;
      console.log("[INFO] Connected to MySQL.");
    }
  });
} catch (e) {
  initSQLite();
}

let sqliteDb;
function initSQLite() {
  try {
    const sqlite3 = require("better-sqlite3");
    const dbPath  = path.join(__dirname, "../data/processed/powergrid_ops.db");
    if (fs.existsSync(dbPath)) {
      sqliteDb = sqlite3(dbPath);
      console.log("[INFO] Using SQLite fallback:", dbPath);
    } else {
      console.warn("[WARN] SQLite DB not found. Run scripts/etl.py first.");
    }
  } catch (e2) {
    console.warn("[WARN] Neither MySQL nor SQLite available:", e2.message);
  }
}

function query(sql, params, callback) {
  if (useMySQL) {
    db.query(sql, params, callback);
  } else if (sqliteDb) {
    try {
      // Convert MySQL placeholder ? to positional and run
      const rows = sqliteDb.prepare(sql.replace(/\?/g, "?")).all(...(params || []));
      callback(null, rows);
    } catch (err) {
      callback(err, null);
    }
  } else {
    callback(new Error("No database available"), null);
  }
}

const app = express();
app.use(cors());
app.use(express.json());

// -------------------------------------------------------
// API 1: Health Check
// -------------------------------------------------------
app.get("/health", (req, res) => {
  res.json({ status: "OK", message: "Server is running", time: new Date(), db: useMySQL ? "MySQL" : "SQLite" });
});

// -------------------------------------------------------
// API 2: Get latest 50 records (with optional region filter)
// -------------------------------------------------------
app.get("/data", (req, res) => {
  const region = req.query.region;
  let sql    = "SELECT * FROM power_usage ORDER BY timestamp DESC LIMIT 50";
  let params = [];
  if (region && region !== "All") {
    sql    = "SELECT * FROM power_usage WHERE region = ? ORDER BY timestamp DESC LIMIT 50";
    params = [region];
  }
  query(sql, params, (err, result) => {
    if (err) return res.status(500).json({ error: err.message });
    res.json(result);
  });
});

// -------------------------------------------------------
// API 3: Region-wise average consumption
// -------------------------------------------------------
app.get("/region", (req, res) => {
  query(
    "SELECT region, ROUND(AVG(consumption),2) as avg_usage, COUNT(*) as total FROM power_usage GROUP BY region",
    [], (err, result) => {
      if (err) return res.status(500).json({ error: err.message });
      res.json(result);
    }
  );
});

// -------------------------------------------------------
// API 4: Summary stats
// -------------------------------------------------------
app.get("/summary", (req, res) => {
  query(
    `SELECT
       COUNT(*) as total_records,
       ROUND(AVG(consumption),2) as avg_consumption,
       ROUND(MAX(consumption),2) as max_consumption,
       ROUND(MIN(consumption),2) as min_consumption
     FROM power_usage`,
    [], (err, result) => {
      if (err) return res.status(500).json({ error: err.message });
      res.json(result[0]);
    }
  );
});

// -------------------------------------------------------
// API 5: Alerts — records with load_level = Critical
// -------------------------------------------------------
app.get("/alerts", (req, res) => {
  query(
    "SELECT * FROM power_usage WHERE load_level='Critical' ORDER BY timestamp DESC LIMIT 20",
    [], (err, result) => {
      if (err) return res.status(500).json({ error: err.message });
      res.json(result);
    }
  );
});

// -------------------------------------------------------
// API 6: Hourly trend
// -------------------------------------------------------
app.get("/trend/hourly", (req, res) => {
  query(
    "SELECT hour, ROUND(AVG(consumption),2) as avg_kwh FROM power_usage GROUP BY hour ORDER BY hour",
    [], (err, result) => {
      if (err) return res.status(500).json({ error: err.message });
      res.json(result);
    }
  );
});

const PORT = process.env.PORT || 5000;
app.listen(PORT, () => console.log(`[INFO] Power Grid API running on port ${PORT}`));
