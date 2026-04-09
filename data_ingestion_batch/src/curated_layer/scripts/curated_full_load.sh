#!/usr/bin/env bash
# =============================================================================
# curated_full_load.sh
# Submit the curated-layer full-load PySpark job to YARN.
# =============================================================================

set -euo pipefail

# Resolve the directory that contains this script so that the PySpark file
# can be referenced by absolute path regardless of where the script is
# invoked from.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SPARK_DIR="${SCRIPT_DIR}/../spark"

echo "=== Curated Full Load – submitting Spark job ==="
echo "    Script directory : ${SCRIPT_DIR}"
echo "    Spark scripts dir: ${SPARK_DIR}"
echo ""

spark-submit \
    --master yarn \
    --deploy-mode client \
    --name curated-full-load \
    --conf spark.sql.hive.convertMetastoreParquet=false \
    --conf spark.yarn.submit.waitAppCompletion=true \
    --py-files "${SPARK_DIR}/transformations.py" \
    "${SPARK_DIR}/full_load.py"

echo ""
echo "=== Curated Full Load – complete ==="
