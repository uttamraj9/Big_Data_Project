#!/bin/bash
# =============================================================================
# get_watermark.sh
# =============================================================================
# Called by the Oozie shell action "get-watermark" in the incremental load
# workflow. Queries MAX(Timestamp) from the Hive bronze table and writes the
# result as a Java properties file to stdout.
#
# Oozie captures stdout via <capture-output/> and exposes the key-value pairs
# as workflow action data, accessible in subsequent actions as:
#   ${wf:actionData('get-watermark')['last_value']}
#
# OUTPUT FORMAT (stdout):
#   last_value=2023-08-07 17:40:00.0
#
# If the Hive table is empty or unreachable, falls back to the Unix epoch
# (1970-01-01 00:00:00) so the first run imports all rows.
# =============================================================================

set -euo pipefail

HIVE_DATABASE="bd_class_project"
HIVE_TABLE="cc_fraud_trans"
HIVESERVER2_HOST="${HIVESERVER2_HOST:-ip-172-31-12-74.eu-west-2.compute.internal}"
HIVESERVER2_URL="jdbc:hive2://${HIVESERVER2_HOST}:10000/${HIVE_DATABASE};"
FALLBACK_TIMESTAMP="1970-01-01 00:00:00"

# Query the maximum Timestamp already present in the Hive bronze table.
# This is the watermark: Sqoop will only import rows newer than this value.
LAST_VALUE=$(beeline \
  -u "${HIVESERVER2_URL}" \
  --silent=true \
  --showHeader=false \
  --outputformat=tsv2 \
  -e "SELECT COALESCE(MAX(\`Timestamp\`), '${FALLBACK_TIMESTAMP}') FROM ${HIVE_TABLE};" \
  2>/dev/null | tail -n 1)

# Fall back to epoch if beeline returned nothing (table empty or unreachable)
if [ -z "${LAST_VALUE}" ]; then
    LAST_VALUE="${FALLBACK_TIMESTAMP}"
fi

# Write as a Java properties file — Oozie captures this from stdout
echo "last_value=${LAST_VALUE}"
