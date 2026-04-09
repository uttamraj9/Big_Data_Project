#!/bin/bash
# =============================================================================
# raw_full_load.sh
# =============================================================================
# Raw Layer — Full Load: PostgreSQL -> HDFS -> Hive External Table
#
# PURPOSE
# -------
# Performs a one-time full load of the cc_fraud_trans table from PostgreSQL
# into the HDFS bronze landing zone and creates (or recreates) the Hive
# external table on top of it.
#
# Run this script once during initial pipeline setup, or whenever a full
# data refresh of the raw layer is required. For ongoing daily updates
# use raw_incremental_load.sh instead.
#
# PIPELINE FLOW
# -------------
#   PostgreSQL (testdb.cc_fraud_trans)
#       |
#       | Sqoop import (--delete-target-dir, 1 mapper, textfile)
#       v
#   HDFS: /tmp/US_UK_05052025/class_project/input/raw_data_sqoop/
#       |
#       | Hive DDL (EXTERNAL TABLE, TEXTFILE, comma-delimited)
#       v
#   Hive: bd_class_project.cc_fraud_trans
#
# PREREQUISITES
# -------------
#   - Sqoop, beeline, and hdfs CLI available on PATH
#   - PostgreSQL JDBC driver accessible by Sqoop
#   - HiveServer2 running at ip-172-31-12-74.eu-west-2.compute.internal:10000
#   - Environment variables set (see below) or a .env file exported beforehand
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
#   bash raw_full_load.sh
#
# =============================================================================

set -euo pipefail

# ── Configuration ─────────────────────────────────────────────────────────────
PG_TABLE="cc_fraud_trans"
HIVE_DATABASE="bd_class_project"
HIVE_TABLE="cc_fraud_trans"
HDFS_TARGET_DIR="/tmp/US_UK_05052025/class_project/input/raw_data_sqoop"
HIVESERVER2_URL="jdbc:hive2://ip-172-31-12-74.eu-west-2.compute.internal:10000"

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

echo "============================================================"
echo " Raw Layer — Full Load"
echo " Source : jdbc:postgresql://${PG_HOST}:5432/${PG_DB}"
echo " Table  : ${PG_TABLE}"
echo " HDFS   : ${HDFS_TARGET_DIR}"
echo " Hive   : ${HIVE_DATABASE}.${HIVE_TABLE}"
echo "============================================================"

# ── Step 1: Prepare HDFS bronze landing directory ─────────────────────────────
echo ""
echo "[Step 1/4] Preparing HDFS bronze landing directory..."

hdfs dfs -mkdir -p "${HDFS_TARGET_DIR}"

# Clear any data from a previous full load so this run is a true replace
hdfs dfs -rm -r -skipTrash "${HDFS_TARGET_DIR}/*" 2>/dev/null || true

# Open permissions so Hive and other cluster services can read the files
sudo -u hdfs hdfs dfs -chmod -R 777 "${HDFS_TARGET_DIR}"

echo "HDFS directory ready: ${HDFS_TARGET_DIR}"

# ── Step 2: Sqoop full import from PostgreSQL to HDFS ─────────────────────────
echo ""
echo "[Step 2/4] Running Sqoop full import (this submits a MapReduce job)..."

sqoop import \
  --connect "jdbc:postgresql://${PG_HOST}:5432/${PG_DB}" \
  --username "${PG_USER}" \
  --password "${PG_PASS}" \
  --table    "${PG_TABLE}" \
  --delete-target-dir \
  --target-dir "${HDFS_TARGET_DIR}" \
  --m 1 \
  --as-textfile \
  --null-string     '\\N' \
  --null-non-string '\\N'

echo "Sqoop import complete. Data written to ${HDFS_TARGET_DIR}"

# ── Step 3: Create Hive database and external table ───────────────────────────
echo ""
echo "[Step 3/4] Creating Hive database and external table..."

# Create the database if it does not exist
beeline -u "${HIVESERVER2_URL}/" --silent=true \
  -e "CREATE DATABASE IF NOT EXISTS ${HIVE_DATABASE};" \
  2>&1 | grep -vE "SLF4J|log4j|WARN|INFO"

# Write the CREATE TABLE DDL to a temp file to avoid shell-escaping issues
# with backtick-quoted reserved words (e.g. `Timestamp`)
HQL_TMP=$(mktemp /tmp/bronze_create_table_XXXX.hql)

cat > "${HQL_TMP}" << HQLEOF
USE ${HIVE_DATABASE};

-- Drop and recreate so schema is always in sync with the source
DROP TABLE IF EXISTS ${HIVE_TABLE};

-- EXTERNAL table: Hive manages only the metadata; the files in HDFS are
-- owned by the ingestion pipeline and will not be deleted if the table is dropped.
CREATE EXTERNAL TABLE IF NOT EXISTS ${HIVE_DATABASE}.${HIVE_TABLE} (
    Transaction_ID               STRING,
    User_ID                      STRING,
    Transaction_Amount           DECIMAL(10,2),
    Transaction_Type             STRING,
    \`Timestamp\`                TIMESTAMP,
    Account_Balance              DECIMAL(10,2),
    Device_Type                  STRING,
    Location                     STRING,
    Merchant_Category            STRING,
    IP_Address_Flag              INT,
    Previous_Fraudulent_Activity INT,
    Daily_Transaction_Count      INT,
    Avg_Transaction_Amount_7d    DECIMAL(10,2),
    Failed_Transaction_Count_7d  INT,
    Card_Type                    STRING,
    Card_Age                     INT,
    Transaction_Distance         DECIMAL(10,2),
    Authentication_Method        STRING,
    Risk_Score                   DECIMAL(5,4),
    Is_Weekend                   INT,
    Fraud_Label                  INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '${HDFS_TARGET_DIR}'
TBLPROPERTIES ("skip.header.line.count"="0");
HQLEOF

beeline -u "${HIVESERVER2_URL}/" --silent=true -f "${HQL_TMP}" \
  2>&1 | grep -vE "SLF4J|log4j|WARN|INFO"

rm -f "${HQL_TMP}"

echo "Hive table ${HIVE_DATABASE}.${HIVE_TABLE} created."

# ── Step 4: Verify row count in Hive ──────────────────────────────────────────
echo ""
echo "[Step 4/4] Verifying row count in Hive..."

ROW_COUNT=$(beeline \
  -u "${HIVESERVER2_URL}/${HIVE_DATABASE};" \
  --silent=true --showHeader=false --outputformat=tsv2 \
  -e "SELECT COUNT(*) FROM ${HIVE_TABLE};" 2>/dev/null | tail -n 1)

echo "Rows in ${HIVE_DATABASE}.${HIVE_TABLE}: ${ROW_COUNT}"

echo ""
echo "============================================================"
echo " Raw Full Load Complete"
echo " Hive table : ${HIVE_DATABASE}.${HIVE_TABLE}"
echo " Rows loaded: ${ROW_COUNT}"
echo "============================================================"
