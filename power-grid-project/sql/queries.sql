-- =========================================================
-- Task 6: SQL Basics - Power Grid Student/Usage Database
-- Task 7: Advanced SQL - Joins, Subqueries, Window Functions
-- Project: Power Grid Analytics Dashboard
-- =========================================================

-- =========================================================
-- TASK 6: SQL BASICS
-- =========================================================

-- Create the database
CREATE DATABASE IF NOT EXISTS powergrid;
USE powergrid;

-- Create main power usage table
CREATE TABLE IF NOT EXISTS power_usage (
    id          INT AUTO_INCREMENT PRIMARY KEY,
    timestamp   DATETIME NOT NULL,
    region      VARCHAR(20) NOT NULL,
    consumption FLOAT NOT NULL,
    voltage     FLOAT DEFAULT 230.0,
    frequency   FLOAT DEFAULT 50.0
);

-- Create regions reference table
CREATE TABLE IF NOT EXISTS regions (
    region_id   INT AUTO_INCREMENT PRIMARY KEY,
    region_name VARCHAR(20) UNIQUE NOT NULL,
    zone        VARCHAR(10) NOT NULL,
    capacity_mw FLOAT NOT NULL
);

-- Create alerts table
CREATE TABLE IF NOT EXISTS alerts (
    alert_id    INT AUTO_INCREMENT PRIMARY KEY,
    region      VARCHAR(20),
    alert_type  VARCHAR(50),
    alert_msg   TEXT,
    created_at  DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Insert sample regions
INSERT IGNORE INTO regions (region_name, zone, capacity_mw) VALUES
    ('North', 'Zone-A', 1500.0),
    ('South', 'Zone-B', 1200.0),
    ('East',  'Zone-C', 900.0),
    ('West',  'Zone-D', 1100.0);

-- Basic SELECT
SELECT * FROM power_usage ORDER BY timestamp DESC LIMIT 10;

-- Filter with WHERE
SELECT * FROM power_usage WHERE region = 'North' AND consumption > 300;

-- GROUP BY region
SELECT region, COUNT(*) AS total_records, ROUND(AVG(consumption), 2) AS avg_consumption
FROM power_usage GROUP BY region;

-- ORDER BY consumption descending
SELECT timestamp, region, consumption FROM power_usage ORDER BY consumption DESC LIMIT 5;

-- =========================================================
-- TASK 7: ADVANCED SQL
-- =========================================================

-- JOIN: power_usage with regions table
SELECT pu.id, pu.timestamp, pu.region, pu.consumption, r.zone, r.capacity_mw,
       ROUND((pu.consumption / r.capacity_mw) * 100, 2) AS utilization_pct
FROM power_usage pu
JOIN regions r ON pu.region = r.region_name
ORDER BY utilization_pct DESC LIMIT 20;

-- SUBQUERY: Records above overall average consumption
SELECT timestamp, region, consumption FROM power_usage
WHERE consumption > (SELECT AVG(consumption) FROM power_usage)
ORDER BY consumption DESC;

-- SUBQUERY: Regions with avg consumption above 300
SELECT region, avg_cons FROM (
    SELECT region, ROUND(AVG(consumption), 2) AS avg_cons
    FROM power_usage GROUP BY region
) AS region_summary WHERE avg_cons > 300;

-- WINDOW FUNCTIONS: Running total + moving avg + rank
SELECT
    timestamp, region, consumption,
    ROUND(SUM(consumption) OVER (PARTITION BY region ORDER BY timestamp), 2) AS running_total,
    ROUND(AVG(consumption) OVER (PARTITION BY region ORDER BY timestamp ROWS BETWEEN 4 PRECEDING AND CURRENT ROW), 2) AS moving_avg_5,
    RANK() OVER (PARTITION BY region ORDER BY consumption DESC) AS consumption_rank
FROM power_usage ORDER BY region, timestamp;

-- Top 3 peak usage per region
SELECT * FROM (
    SELECT timestamp, region, consumption,
        RANK() OVER (PARTITION BY region ORDER BY consumption DESC) AS rnk
    FROM power_usage
) ranked WHERE rnk <= 3;

-- Peak vs Off-peak, load level classification
SELECT timestamp, region, consumption,
    HOUR(timestamp) AS hour_of_day,
    CASE WHEN HOUR(timestamp) BETWEEN 8 AND 20 THEN 'Peak' ELSE 'Off-Peak' END AS period,
    CASE
        WHEN consumption > 400 THEN 'Critical'
        WHEN consumption BETWEEN 300 AND 400 THEN 'High'
        WHEN consumption BETWEEN 200 AND 300 THEN 'Normal'
        ELSE 'Low'
    END AS load_level
FROM power_usage ORDER BY timestamp DESC LIMIT 30;

-- Monthly Region Business Report
SELECT region, DATE_FORMAT(timestamp, '%Y-%m') AS month,
    COUNT(*) AS records,
    ROUND(AVG(consumption), 2) AS avg_consumption,
    ROUND(MAX(consumption), 2) AS peak_consumption,
    ROUND(SUM(consumption), 2) AS total_consumption
FROM power_usage
GROUP BY region, DATE_FORMAT(timestamp, '%Y-%m')
ORDER BY month, region;

-- ALERT TRIGGER: Insert alert when consumption > 480
INSERT INTO alerts (region, alert_type, alert_msg)
SELECT DISTINCT region, 'HIGH_LOAD',
    CONCAT('Critical consumption detected in ', region, ' region')
FROM power_usage WHERE consumption > 480;

-- Average Consumption
SELECT AVG(consumption) AS avg_consumption FROM power_usage;

-- Peak Usage
SELECT MAX(consumption) AS peak_usage FROM power_usage;

-- Region-wise Consumption
SELECT region, AVG(consumption) AS avg_usage FROM power_usage GROUP BY region;

-- Daily Trend
SELECT DATE(timestamp) AS day, AVG(consumption) AS avg_usage
FROM power_usage GROUP BY day;

-- Total Records
SELECT COUNT(*) FROM power_usage;

-- TASK 8: OLTP vs OLAP comparison
-- OLTP example (operational, row-level inserts/updates)
INSERT INTO power_usage (timestamp, region, consumption) VALUES (NOW(), 'North', 312.5);
UPDATE power_usage SET consumption = 315.0 WHERE id = 1;

-- OLAP example (analytical, aggregations across large data)
SELECT region,
    DATE_FORMAT(timestamp, '%Y-%m') AS month,
    ROUND(SUM(consumption), 2) AS total_kwh,
    ROUND(AVG(consumption), 2) AS avg_kwh,
    ROUND(MAX(consumption), 2) AS peak_kwh
FROM power_usage
GROUP BY region, DATE_FORMAT(timestamp, '%Y-%m')
WITH ROLLUP;
