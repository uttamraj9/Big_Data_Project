#!/bin/bash
# =============================================================================
# deploy.sh
# =============================================================================
# Deploys the Oozie raw layer workflows to HDFS and optionally runs them.
#
# RUN THIS SCRIPT FROM THE CLOUDERA EDGE NODE (13.41.167.97) where the
# hdfs, oozie, and beeline CLI tools are available.
#
# WHAT IT DOES
# ------------
#   1. Uploads full_load workflow files  -> HDFS /user/ec2-user/oozie/raw_full_load/
#   2. Uploads incremental_load files    -> HDFS /user/ec2-user/oozie/raw_incremental_load/
#   3. Runs the full load workflow once  (--run-full flag)
#   4. Submits the incremental load coordinator (--run-incremental flag)
#
# USAGE
# -----
#   # Deploy only (no job submission)
#   bash deploy.sh
#
#   # Deploy and run the full load immediately
#   bash deploy.sh --run-full
#
#   # Deploy and schedule the daily incremental coordinator
#   bash deploy.sh --run-incremental
#
#   # Deploy and run both
#   bash deploy.sh --run-full --run-incremental
# =============================================================================

set -euo pipefail

OOZIE_URL="http://ip-172-31-12-74.eu-west-2.compute.internal:11000/oozie"
HDFS_BASE="/user/ec2-user/oozie"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

RUN_FULL=false
RUN_INCREMENTAL=false

# Parse flags
for arg in "$@"; do
    case $arg in
        --run-full)         RUN_FULL=true ;;
        --run-incremental)  RUN_INCREMENTAL=true ;;
    esac
done

echo "============================================================"
echo " Raw Layer Oozie Deploy"
echo " HDFS base : ${HDFS_BASE}"
echo " Oozie URL : ${OOZIE_URL}"
echo "============================================================"

# ── Step 1: Upload full load workflow to HDFS ─────────────────────────────────
echo ""
echo "[1/2] Uploading full_load workflow to HDFS..."

FULL_LOAD_HDFS="${HDFS_BASE}/raw_full_load"
hdfs dfs -mkdir -p "${FULL_LOAD_HDFS}"

hdfs dfs -put -f "${SCRIPT_DIR}/full_load/workflow.xml"                "${FULL_LOAD_HDFS}/"
hdfs dfs -put -f "${SCRIPT_DIR}/full_load/job.properties"              "${FULL_LOAD_HDFS}/"
hdfs dfs -put -f "${SCRIPT_DIR}/full_load/create_raw_hive_table.hql" "${FULL_LOAD_HDFS}/"

echo "  Uploaded: ${FULL_LOAD_HDFS}/"
hdfs dfs -ls "${FULL_LOAD_HDFS}/"

# ── Step 2: Upload incremental load workflow + coordinator to HDFS ────────────
echo ""
echo "[2/2] Uploading incremental_load workflow to HDFS..."

INC_LOAD_HDFS="${HDFS_BASE}/raw_incremental_load"
hdfs dfs -mkdir -p "${INC_LOAD_HDFS}"

hdfs dfs -put -f "${SCRIPT_DIR}/incremental_load/workflow.xml"    "${INC_LOAD_HDFS}/"
hdfs dfs -put -f "${SCRIPT_DIR}/incremental_load/coordinator.xml" "${INC_LOAD_HDFS}/"
hdfs dfs -put -f "${SCRIPT_DIR}/incremental_load/job.properties"  "${INC_LOAD_HDFS}/"

# Upload the get_watermark.sh shell script — referenced by the shell action
hdfs dfs -put -f "${SCRIPT_DIR}/incremental_load/scripts/get_watermark.sh" "${INC_LOAD_HDFS}/"
hdfs dfs -chmod 755 "${INC_LOAD_HDFS}/get_watermark.sh"

echo "  Uploaded: ${INC_LOAD_HDFS}/"
hdfs dfs -ls "${INC_LOAD_HDFS}/"

echo ""
echo "============================================================"
echo " Deploy complete."
echo "============================================================"

# ── Optional: Submit full load workflow ───────────────────────────────────────
if [ "${RUN_FULL}" = true ]; then
    echo ""
    echo "Submitting raw full load workflow..."
    JOB_ID=$(oozie job -run \
        -oozie "${OOZIE_URL}" \
        -config "${SCRIPT_DIR}/full_load/job.properties" \
        2>&1 | grep "job:" | awk '{print $2}')
    echo "  Submitted. Job ID: ${JOB_ID}"
    echo "  Track at: ${OOZIE_URL}/?job=${JOB_ID}"
    echo ""
    echo "  Polling status..."
    for i in {1..24}; do
        STATUS=$(oozie job -info "${JOB_ID}" -oozie "${OOZIE_URL}" 2>/dev/null | grep "Status" | head -1 | awk '{print $3}')
        echo "  [${i}] ${STATUS}"
        if [[ "${STATUS}" == "SUCCEEDED" || "${STATUS}" == "FAILED" || "${STATUS}" == "KILLED" ]]; then
            break
        fi
        sleep 10
    done
fi

# ── Optional: Submit incremental load coordinator ─────────────────────────────
if [ "${RUN_INCREMENTAL}" = true ]; then
    echo ""
    echo "Submitting raw incremental load coordinator (daily at 10:00 AM UTC)..."
    COORD_ID=$(oozie job -run \
        -oozie "${OOZIE_URL}" \
        -config "${SCRIPT_DIR}/incremental_load/job.properties" \
        2>&1 | grep "job:" | awk '{print $2}')
    echo "  Submitted. Coordinator ID: ${COORD_ID}"
    echo "  Track at: ${OOZIE_URL}/?job=${COORD_ID}"
fi
