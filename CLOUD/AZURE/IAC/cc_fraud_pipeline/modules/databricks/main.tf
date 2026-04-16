terraform {
  required_providers {
    azurerm = { source = "hashicorp/azurerm" }
    null    = { source = "hashicorp/null" }
    local   = { source = "hashicorp/local" }
  }
}

# ─── Databricks workspace ─────────────────────────────────────
resource "azurerm_databricks_workspace" "dbw" {
  name                        = var.databricks_workspace_name
  resource_group_name         = var.resource_group_name
  location                    = var.location
  sku                         = "premium"
  managed_resource_group_name = "databricks-rg-${var.databricks_workspace_name}"
}

# ─── Job configs written to /tmp for curl provisioners ───────
locals {
  raw_to_curated_job = jsonencode({
    name = "cc-fraud-raw-to-curated"
    tasks = [{
      task_key = "raw_to_curated"
      notebook_task = {
        notebook_path = "/Shared/cc_fraud_pipeline/raw_to_curated"
        base_parameters = {
          adls_account_name = var.adls_account_name
          raw_container     = var.raw_container_name
          curated_container = var.curated_container_name
          table_name        = "cc_fraud_trans"
        }
      }
      new_cluster = {
        num_workers   = 1
        spark_version = "13.3.x-scala2.12"
        node_type_id  = "Standard_DS3_v2"
        azure_attributes = {
          availability    = "ON_DEMAND_AZURE"
          first_on_demand = 1
        }
        spark_conf = {
          "spark.databricks.delta.preview.enabled" = "true"
        }
        spark_env_vars = {
          ADLS_ACCOUNT_NAME = var.adls_account_name
          RAW_CONTAINER     = var.raw_container_name
          CURATED_CONTAINER = var.curated_container_name
          GOLD_CONTAINER    = var.gold_container_name
        }
      }
    }]
    schedule = {
      quartz_cron_expression = "0 30 2 * * ?"
      timezone_id            = "UTC"
      pause_status           = "UNPAUSED"
    }
  })

  curated_to_gold_job = jsonencode({
    name = "cc-fraud-curated-to-gold"
    tasks = [{
      task_key = "curated_to_gold"
      notebook_task = {
        notebook_path = "/Shared/cc_fraud_pipeline/curated_to_gold"
        base_parameters = {
          adls_account_name = var.adls_account_name
          curated_container = var.curated_container_name
          gold_container    = var.gold_container_name
          table_name        = "cc_fraud_trans"
        }
      }
      new_cluster = {
        num_workers   = 1
        spark_version = "13.3.x-scala2.12"
        node_type_id  = "Standard_DS3_v2"
        azure_attributes = {
          availability    = "ON_DEMAND_AZURE"
          first_on_demand = 1
        }
        spark_conf = {
          "spark.databricks.delta.preview.enabled" = "true"
        }
        spark_env_vars = {
          ADLS_ACCOUNT_NAME = var.adls_account_name
          CURATED_CONTAINER = var.curated_container_name
          GOLD_CONTAINER    = var.gold_container_name
        }
      }
    }]
    schedule = {
      quartz_cron_expression = "0 30 3 * * ?"
      timezone_id            = "UTC"
      pause_status           = "UNPAUSED"
    }
  })
}

resource "local_file" "raw_to_curated_json" {
  filename = "/tmp/cc_fraud_raw_to_curated_job.json"
  content  = local.raw_to_curated_job
}

resource "local_file" "curated_to_gold_json" {
  filename = "/tmp/cc_fraud_curated_to_gold_job.json"
  content  = local.curated_to_gold_job
}

# ─── Helper: create-or-update a job by name via REST API ─────
# Uses Databricks 2.1 API with Azure AD token (resource: 2ff814a6-...)
resource "null_resource" "raw_to_curated_job" {
  triggers = {
    workspace_url = azurerm_databricks_workspace.dbw.workspace_url
    job_hash      = sha256(local.raw_to_curated_job)
  }

  provisioner "local-exec" {
    command = <<-CMD
      TOKEN=$(az account get-access-token --resource 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d --query accessToken -o tsv)
      WS="https://${azurerm_databricks_workspace.dbw.workspace_url}"
      JOB_ID=$(curl -sf -H "Authorization: Bearer $TOKEN" "$WS/api/2.1/jobs/list?name=cc-fraud-raw-to-curated&limit=1" \
        | python3 -c "import sys,json; d=json.load(sys.stdin); j=d.get('jobs',[]); print(j[0]['job_id'] if j else '')" 2>/dev/null || echo "")
      if [ -n "$JOB_ID" ]; then
        curl -sf -X POST -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
          "$WS/api/2.1/jobs/reset" \
          -d "{\"job_id\":$JOB_ID,\"new_settings\":$(cat ${local_file.raw_to_curated_json.filename})}" > /dev/null
        echo "Updated job $JOB_ID"
      else
        curl -sf -X POST -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
          "$WS/api/2.1/jobs/create" \
          -d @${local_file.raw_to_curated_json.filename}
        echo "Created job cc-fraud-raw-to-curated"
      fi
    CMD
  }

  provisioner "local-exec" {
    when = destroy
    command = <<-CMD
      TOKEN=$(az account get-access-token --resource 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d --query accessToken -o tsv 2>/dev/null) || exit 0
      WS="https://${self.triggers.workspace_url}"
      JOB_ID=$(curl -sf -H "Authorization: Bearer $TOKEN" "$WS/api/2.1/jobs/list?name=cc-fraud-raw-to-curated&limit=1" \
        | python3 -c "import sys,json; d=json.load(sys.stdin); j=d.get('jobs',[]); print(j[0]['job_id'] if j else '')" 2>/dev/null || echo "")
      [ -n "$JOB_ID" ] && curl -sf -X POST -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
        "$WS/api/2.1/jobs/delete" -d "{\"job_id\":$JOB_ID}" || true
    CMD
  }

  depends_on = [
    azurerm_databricks_workspace.dbw,
    local_file.raw_to_curated_json,
  ]
}

resource "null_resource" "curated_to_gold_job" {
  triggers = {
    workspace_url = azurerm_databricks_workspace.dbw.workspace_url
    job_hash      = sha256(local.curated_to_gold_job)
  }

  provisioner "local-exec" {
    command = <<-CMD
      TOKEN=$(az account get-access-token --resource 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d --query accessToken -o tsv)
      WS="https://${azurerm_databricks_workspace.dbw.workspace_url}"
      JOB_ID=$(curl -sf -H "Authorization: Bearer $TOKEN" "$WS/api/2.1/jobs/list?name=cc-fraud-curated-to-gold&limit=1" \
        | python3 -c "import sys,json; d=json.load(sys.stdin); j=d.get('jobs',[]); print(j[0]['job_id'] if j else '')" 2>/dev/null || echo "")
      if [ -n "$JOB_ID" ]; then
        curl -sf -X POST -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
          "$WS/api/2.1/jobs/reset" \
          -d "{\"job_id\":$JOB_ID,\"new_settings\":$(cat ${local_file.curated_to_gold_json.filename})}" > /dev/null
        echo "Updated job $JOB_ID"
      else
        curl -sf -X POST -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
          "$WS/api/2.1/jobs/create" \
          -d @${local_file.curated_to_gold_json.filename}
        echo "Created job cc-fraud-curated-to-gold"
      fi
    CMD
  }

  provisioner "local-exec" {
    when = destroy
    command = <<-CMD
      TOKEN=$(az account get-access-token --resource 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d --query accessToken -o tsv 2>/dev/null) || exit 0
      WS="https://${self.triggers.workspace_url}"
      JOB_ID=$(curl -sf -H "Authorization: Bearer $TOKEN" "$WS/api/2.1/jobs/list?name=cc-fraud-curated-to-gold&limit=1" \
        | python3 -c "import sys,json; d=json.load(sys.stdin); j=d.get('jobs',[]); print(j[0]['job_id'] if j else '')" 2>/dev/null || echo "")
      [ -n "$JOB_ID" ] && curl -sf -X POST -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
        "$WS/api/2.1/jobs/delete" -d "{\"job_id\":$JOB_ID}" || true
    CMD
  }

  depends_on = [
    azurerm_databricks_workspace.dbw,
    local_file.curated_to_gold_json,
  ]
}
