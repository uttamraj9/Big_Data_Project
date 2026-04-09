#!/bin/bash
# =============================================================================
# bronze_incremental_load.sh
# =============================================================================
# Bronze Layer — Incremental Load: PostgreSQL -> HDFS (append)
#
# PURPOSE
# -------
# Appends only new records — those with a Timestamp greater than the current
# maximum in the Hive bronze table — from PostgreSQL into the HDFS landing
# zone. Triggered daily by Jenkins (see Jenkinsfile).
#
# This simulates a real incremental ETL pattern where only the delta (new
# transactions since the last run) is transferred, keeping HDFS loads small
# and the Hive table continuously up to date without re-ingesting history.
#
# PIPELINE FLOW
# -------------
#   Hive: bd_class_project.cc_fraud_trans  <-- query MAX(Timestamp) watermark
#       |
#       | Sqoop import (--incremental append, --check-column Timestamp)
#       v
#   HDFS: /tmp/US_UK_05052025/class_project/input/raw_data_sqoop/  (appended)
#       |
#       | Hive external table reads new files automatically (no DDL change)
#       v
#   Hive: bd_class_project.cc_fraud_trans  (updated)
#
# PREREQUISITES
# -------------
#   - bronze_full_load.sh must have been run at least once to create the
#     Hive table and seed the initial dataset.
#   - Sqoop, beeline, and hdfs CLI available on PATH
#   - HiveServer2 running at ip-172-31-12-74.eu-west-2.compute.internal:10000
#   - Environment variables set (see below)
#
# ENVIRONMENT VARIABLES (required)
# ---------------------------------
#   DB_HOST        PostgreSQL hostname / IP
#   DB_NAME        PostgreSQL database name
#   DB_USERNAME    PostgreSQL user
#   DB_PASSWORD    PostgreSQL password
#
# USAGE
# -----
#   export DB_HOST=<host> DB_NAME=<db> DB_USERNAME=<user> DB_PASSWORD=<pass>
#   bash bronze_incremental_load.sh
#
# SCHEDULING
# ----------
#   Automated via Jenkins cron: runs daily at 10:00 AM (see Jenkinsfile).
#   The Jenkins job also runs ingest_to_postgres.py inc beforehand to
#   simulate new records arriving in the source PostgreSQL database.
#
# =============================================================================

set -euo pipefail

# ── Configuration ─────────────────────────────────────────────────────────────
PG_TABLE="cc_fraud_trans"
HIVE_DATABASE="bd_class_project"
HIVE_TABLE="cc_fraud_trans"
HDFS_TARGET_DIR="/tmp/US_UK_05052025/class_project/input/raw_data_sqoop"
HIVESERVER2_URL="jdbc:hive2://ip-172-31-12-74.eu-west-2.compute.internal:10000/${HIVE_DATABASE};"

# ── Read DB connection from environment ───────────────────────────────────────
PG_HOST="${DB_HOST}"
PG_DB="${DB_NAME}"
PG_USER="${DB_USERNAME}"
PG_PASS="${DB_PASSWORD}"

# ── Validate required environment variables ───────────────────────────────────
for var in DB_HOST DB_NAME DB_USERNAME DB_PASSWORD; do
    if [ -z "${!var}" ]; then
        echo "ERROR: Required environment variable '${var}' is not set."
        exit 1
    fi
done

# ── Ensure HDFS directory is accessible ───────────────────────────────────────
sudo -u hdfs hdfs dfs -chmod -R 777 "${HDFS_TARGET_DIR}"
sudo -u hdfs hdfs dfs -chmod -R 777 "${HDFS_TARGET_DIR}/*" 2>/dev/null || true

# ── Step 1: Read the watermark (max Timestamp) from Hive ──────────────────────
echo "[Step 1/2] Fetching watermark from Hive table ${HIVE_DATABASE}.${HIVE_TABLE}..."

LAST_VALUE=$(beeline -u "${HIVESERVER2_URL}" \
  --silent=true --showHeader=false --outputformat=tsv2 \
  -e "SELECT COALESCE(MAX(\`Timestamp\`), '1970-01-01 00:00:00') FROM ${HIVE_TABLE};" \
  2>/dev/null | tail -n 1)

if [ -z "${LAST_VALUE}" ]; then
    echo "ERROR: Could not retrieve watermark from Hive. Is the table populated?"
    echo "       Run bronze_full_load.sh first."
    exit 1
fi

echo "Watermark: ${LAST_VALUE} — importing records newer than this timestamp."

# ── Step 2: Sqoop incremental import (append only new rows) ───────────────────
echo ""
echo "[Step 2/2] Running Sqoop incremental import..."

sqoop import \
  --connect "jdbc:postgresql://${PG_HOST}:5432/${PG_DB}" \
  --username "${PG_USER}" \
  --password "${PG_PASS}" \
  --table    "${PG_TABLE}" \
  --incremental append \
  --check-column Timestamp \
  --last-value   "${LAST_VALUE}" \
  --target-dir   "${HDFS_TARGET_DIR}" \
  --m 1 \
  --as-textfile

echo ""
echo "Incremental load complete."
echo "New records are now available in Hive table ${HIVE_DATABASE}.${HIVE_TABLE}"
