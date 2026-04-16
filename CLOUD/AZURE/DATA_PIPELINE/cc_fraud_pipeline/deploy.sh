#!/usr/bin/env bash
# deploy.sh — Deploy cc_fraud_pipeline data pipeline artifacts to Azure
# Usage: ./deploy.sh
# Requires: az CLI logged in, Databricks workspace reachable

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ─── Config (matches terraform.tfvars) ───────────────────────
RG="Itc_Bigdata"
ADF_NAME="itc-bd-ne-adf"
ADB_WORKSPACE_URL="https://adb-7405609294150794.14.azuredatabricks.net"
RAW_JOB_ID=899496009869522
GOLD_JOB_ID=671241738264117

# ─── ADF: Datasets ───────────────────────────────────────────
echo "[1/5] Deploying ADF datasets..."
az datafactory dataset create \
  --resource-group "$RG" --factory-name "$ADF_NAME" \
  --dataset-name "DS_CC_Fraud_Trans_PG" \
  --properties @"$SCRIPT_DIR/adf/datasets/DS_CC_Fraud_Trans_PG.json"

az datafactory dataset create \
  --resource-group "$RG" --factory-name "$ADF_NAME" \
  --dataset-name "DS_CC_Fraud_Trans_ADLS_Raw" \
  --properties @"$SCRIPT_DIR/adf/datasets/DS_CC_Fraud_Trans_ADLS_Raw.json"

# ─── ADF: Pipeline ───────────────────────────────────────────
echo "[2/5] Deploying ADF pipeline..."
az datafactory pipeline create \
  --resource-group "$RG" --factory-name "$ADF_NAME" \
  --name "PL_CC_Fraud_Trans_PG_To_Raw" \
  --pipeline @"$SCRIPT_DIR/adf/pipelines/PL_CC_Fraud_Trans_PG_To_Raw.json"

# ─── ADF: Trigger ────────────────────────────────────────────
echo "[3/5] Deploying ADF trigger..."
# Stop trigger if running before updating
az datafactory trigger stop \
  --resource-group "$RG" --factory-name "$ADF_NAME" \
  --name "TR_CC_Fraud_Daily_Ingest" 2>/dev/null || true

az datafactory trigger create \
  --resource-group "$RG" --factory-name "$ADF_NAME" \
  --name "TR_CC_Fraud_Daily_Ingest" \
  --properties @"$SCRIPT_DIR/adf/triggers/TR_CC_Fraud_Daily_Ingest.json"

az datafactory trigger start \
  --resource-group "$RG" --factory-name "$ADF_NAME" \
  --name "TR_CC_Fraud_Daily_Ingest"

# ─── Databricks: Upload notebooks ────────────────────────────
echo "[4/5] Uploading Databricks notebooks..."
TOKEN=$(az account get-access-token \
  --resource 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d \
  --query accessToken -o tsv)

curl -sf -X POST -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  "$ADB_WORKSPACE_URL/api/2.0/workspace/mkdirs" \
  -d '{"path": "/Shared/cc_fraud_pipeline"}'

for nb in raw_to_curated curated_to_gold; do
  CONTENT=$(base64 < "$SCRIPT_DIR/databricks/${nb}.py")
  curl -sf -X POST -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
    "$ADB_WORKSPACE_URL/api/2.0/workspace/import" \
    -d "{\"path\":\"/Shared/cc_fraud_pipeline/${nb}\",\"format\":\"SOURCE\",\"language\":\"PYTHON\",\"content\":\"$CONTENT\",\"overwrite\":true}"
  echo "  uploaded: /Shared/cc_fraud_pipeline/${nb}"
done

# ─── Databricks: Update job definitions ──────────────────────
echo "[5/5] Updating Databricks job definitions..."
update_job() {
  local job_id=$1
  local job_file=$2
  curl -sf -X POST -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
    "$ADB_WORKSPACE_URL/api/2.1/jobs/reset" \
    -d "{\"job_id\":${job_id},\"new_settings\":$(cat "$job_file")}"
  echo "  updated job $job_id"
}

update_job "$RAW_JOB_ID"  "$SCRIPT_DIR/databricks/jobs/cc_fraud_raw_to_curated_job.json"
update_job "$GOLD_JOB_ID" "$SCRIPT_DIR/databricks/jobs/cc_fraud_curated_to_gold_job.json"

echo ""
echo "Deploy complete."
