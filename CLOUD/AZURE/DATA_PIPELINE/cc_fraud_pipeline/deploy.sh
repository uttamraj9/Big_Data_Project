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
echo "[1/6] Deploying ADF datasets..."
az datafactory dataset create \
  --resource-group "$RG" --factory-name "$ADF_NAME" \
  --dataset-name "DS_CC_Fraud_Trans_PG" \
  --properties @"$SCRIPT_DIR/adf/datasets/DS_CC_Fraud_Trans_PG.json"

az datafactory dataset create \
  --resource-group "$RG" --factory-name "$ADF_NAME" \
  --dataset-name "DS_CC_Fraud_Trans_ADLS_Raw" \
  --properties @"$SCRIPT_DIR/adf/datasets/DS_CC_Fraud_Trans_ADLS_Raw.json"

az datafactory dataset create \
  --resource-group "$RG" --factory-name "$ADF_NAME" \
  --dataset-name "DS_CC_Fraud_Watermark" \
  --properties @"$SCRIPT_DIR/adf/datasets/DS_CC_Fraud_Watermark.json"

# ─── ADF: Pipeline ───────────────────────────────────────────
echo "[2/6] Deploying ADF pipeline (incremental)..."
az datafactory pipeline create \
  --resource-group "$RG" --factory-name "$ADF_NAME" \
  --name "PL_CC_Fraud_Trans_PG_To_Raw" \
  --pipeline @"$SCRIPT_DIR/adf/pipelines/PL_CC_Fraud_Trans_PG_To_Raw.json"

# ─── ADF: Trigger ────────────────────────────────────────────
echo "[3/6] Deploying ADF trigger..."
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

# ─── Databricks: Get access token ────────────────────────────
TOKEN=$(az account get-access-token \
  --resource 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d \
  --query accessToken -o tsv)

# ─── Databricks: Upload notebooks ────────────────────────────
echo "[4/6] Uploading Databricks notebooks..."
curl -sf -X POST -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  "$ADB_WORKSPACE_URL/api/2.0/workspace/mkdirs" \
  -d '{"path": "/Shared/cc_fraud_pipeline"}'

for nb in init_watermark raw_to_curated curated_to_gold; do
  CONTENT=$(base64 < "$SCRIPT_DIR/databricks/${nb}.py")
  curl -sf -X POST -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
    "$ADB_WORKSPACE_URL/api/2.0/workspace/import" \
    -d "{\"path\":\"/Shared/cc_fraud_pipeline/${nb}\",\"format\":\"SOURCE\",\"language\":\"PYTHON\",\"content\":\"$CONTENT\",\"overwrite\":true}"
  echo "  uploaded: /Shared/cc_fraud_pipeline/${nb}"
done

# ─── Databricks: Initialise watermark (first deploy only) ────
echo "[5/6] Initialising watermark in ADLS (safe to re-run)..."
RUN_ID=$(curl -sf -X POST -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  "$ADB_WORKSPACE_URL/api/2.1/jobs/runs/submit" \
  -d "{
    \"run_name\": \"init-watermark-deploy\",
    \"notebook_task\": {
      \"notebook_path\": \"/Shared/cc_fraud_pipeline/init_watermark\",
      \"base_parameters\": {
        \"adls_account_name\": \"itcbdneadls\",
        \"raw_container\": \"raw\",
        \"table_name\": \"cc_fraud_trans\"
      }
    },
    \"new_cluster\": {
      \"num_workers\": 0,
      \"spark_version\": \"13.3.x-scala2.12\",
      \"node_type_id\": \"Standard_D2ads_v6\",
      \"spark_conf\": {
        \"spark.databricks.cluster.profile\": \"singleNode\",
        \"spark.master\": \"local[*]\"
      },
      \"custom_tags\": { \"ResourceClass\": \"SingleNode\" },
      \"azure_attributes\": { \"availability\": \"ON_DEMAND_AZURE\", \"first_on_demand\": 1 }
    }
  }" | python3 -c "import sys,json; print(json.load(sys.stdin)['run_id'])")

echo "  init_watermark run_id=$RUN_ID (running async, check Databricks Jobs UI)"

# ─── Databricks: Update job definitions ──────────────────────
echo "[6/6] Updating Databricks job definitions..."
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
echo ""
echo "INCREMENTAL PIPELINE FLOW:"
echo "  01:00 UTC — ADF reads watermark from raw/watermark/cc_fraud_trans.json"
echo "               copies WHERE timestamp > last_watermark"
echo "               writes to raw/cc_fraud_trans/ingestion_date=YYYY-MM-DD/"
echo "  02:30 UTC — Databricks raw_to_curated reads today's partition"
echo "               MERGEs into Delta table at curated/cc_fraud_trans/"
echo "               advances watermark to max(timestamp)"
echo "  03:30 UTC — Databricks curated_to_gold reads full Delta table"
echo "               overwrites 5 gold aggregation tables"
