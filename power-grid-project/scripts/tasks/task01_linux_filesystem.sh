#!/bin/bash
# =============================================================
# Task 1: Linux + File System Task
# Project: Power Grid Analytics Dashboard
# Description: Structured directory setup, file permissions,
#              automated file movement, and operation logging.
# =============================================================

LOG_FILE="logs/pipeline_operations.log"
TIMESTAMP=$(date "+%Y-%m-%d %H:%M:%S")

log() {
    echo "[$TIMESTAMP] $1" | tee -a "$LOG_FILE"
}

# --------------------------------------------------
# Step 1: Create structured directory for pipeline
# --------------------------------------------------
echo "========================================"
echo "  Power Grid - Linux File System Setup  "
echo "========================================"

mkdir -p data/raw
mkdir -p data/processed
mkdir -p data/archive
mkdir -p logs
mkdir -p reports
mkdir -p scripts/tasks
mkdir -p config

log "INFO: Directories created for Power Grid Pipeline"

# --------------------------------------------------
# Step 2: Set proper file permissions
# --------------------------------------------------
chmod 755 data/
chmod 755 data/raw
chmod 755 data/processed
chmod 700 data/archive          # Only owner can access archive
chmod 755 logs/
chmod 644 data/*.csv 2>/dev/null || true
chmod 755 scripts/*.py 2>/dev/null || true
chmod 755 scripts/tasks/*.sh 2>/dev/null || true

log "INFO: File permissions set correctly"

# --------------------------------------------------
# Step 3: Check if raw CSV exists, if not create sample
# --------------------------------------------------
RAW_FILE="data/raw/power_data_$(date +%Y%m%d).csv"

if [ ! -f "$RAW_FILE" ]; then
    log "INFO: Raw data file not found. Generating sample..."
    python3 scripts/generate_data.py 2>/dev/null || {
        log "WARN: generate_data.py not found. Creating placeholder CSV."
        echo "timestamp,region,consumption" > "$RAW_FILE"
        echo "$(date),North,250.5" >> "$RAW_FILE"
    }
    cp data/power_data.csv "$RAW_FILE" 2>/dev/null || true
    log "INFO: Raw data file created at $RAW_FILE"
fi

# --------------------------------------------------
# Step 4: Automated file movement - raw → processed
# --------------------------------------------------
PROCESSED_FILE="data/processed/power_data_processed_$(date +%Y%m%d).csv"

if [ -f "$RAW_FILE" ]; then
    cp "$RAW_FILE" "$PROCESSED_FILE"
    log "INFO: File moved from raw to processed: $PROCESSED_FILE"
else
    log "ERROR: Raw file not found, skipping move."
fi

# --------------------------------------------------
# Step 5: Archive files older than 7 days
# --------------------------------------------------
find data/processed/ -name "*.csv" -mtime +7 -exec mv {} data/archive/ \; 2>/dev/null
log "INFO: Old files archived (older than 7 days)"

# --------------------------------------------------
# Step 6: Generate a disk usage report
# --------------------------------------------------
echo ""
echo "======= Disk Usage Report ======="
du -sh data/ logs/ reports/ 2>/dev/null
echo "================================="

log "INFO: Disk usage report generated"

# --------------------------------------------------
# Step 7: List all files with permissions
# --------------------------------------------------
echo ""
echo "======= File Permissions ======="
ls -lR data/ 2>/dev/null | head -30
echo "================================"

log "INFO: Pipeline setup completed successfully"
echo ""
echo "✅ Linux File System Task Complete. Check logs/pipeline_operations.log"
