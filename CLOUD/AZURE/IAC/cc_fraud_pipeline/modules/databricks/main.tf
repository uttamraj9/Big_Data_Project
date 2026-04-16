terraform {
  required_providers {
    azurerm = {
      source = "hashicorp/azurerm"
    }
    databricks = {
      source = "databricks/databricks"
    }
  }
}

# ─── Reference existing Databricks workspace ─────────────────
data "azurerm_databricks_workspace" "dbw" {
  name                = var.databricks_workspace_name
  resource_group_name = var.resource_group_name
}

# ─── Job: Raw → Curated (02:30 UTC) ─────────────────────────
# Uses job clusters (new_cluster) — no persistent cluster needed,
# so Terraform doesn't allocate VMs at plan/apply time.
resource "databricks_job" "raw_to_curated" {
  name = "cc-fraud-raw-to-curated"

  task {
    task_key = "raw_to_curated"

    notebook_task {
      notebook_path = "/Shared/cc_fraud_pipeline/raw_to_curated"
      base_parameters = {
        adls_account_name = var.adls_account_name
        raw_container     = var.raw_container_name
        curated_container = var.curated_container_name
        table_name        = "cc_fraud_trans"
      }
    }

    new_cluster {
      num_workers   = 1
      spark_version = "13.3.x-scala2.12"
      node_type_id  = "Standard_DS3_v2"

      azure_attributes {
        availability    = "ON_DEMAND_AZURE"
        first_on_demand = 1
      }

      spark_conf = {
        "spark.databricks.delta.preview.enabled" = "true"
      }

      spark_env_vars = {
        "ADLS_ACCOUNT_NAME" = var.adls_account_name
        "RAW_CONTAINER"     = var.raw_container_name
        "CURATED_CONTAINER" = var.curated_container_name
        "GOLD_CONTAINER"    = var.gold_container_name
      }
    }
  }

  schedule {
    quartz_cron_expression = "0 30 2 * * ?"
    timezone_id            = "UTC"
  }
}

# ─── Job: Curated → Gold (03:30 UTC) ─────────────────────────
resource "databricks_job" "curated_to_gold" {
  name = "cc-fraud-curated-to-gold"

  task {
    task_key = "curated_to_gold"

    notebook_task {
      notebook_path = "/Shared/cc_fraud_pipeline/curated_to_gold"
      base_parameters = {
        adls_account_name = var.adls_account_name
        curated_container = var.curated_container_name
        gold_container    = var.gold_container_name
        table_name        = "cc_fraud_trans"
      }
    }

    new_cluster {
      num_workers   = 1
      spark_version = "13.3.x-scala2.12"
      node_type_id  = "Standard_DS3_v2"

      azure_attributes {
        availability    = "ON_DEMAND_AZURE"
        first_on_demand = 1
      }

      spark_conf = {
        "spark.databricks.delta.preview.enabled" = "true"
      }

      spark_env_vars = {
        "ADLS_ACCOUNT_NAME" = var.adls_account_name
        "CURATED_CONTAINER" = var.curated_container_name
        "GOLD_CONTAINER"    = var.gold_container_name
      }
    }
  }

  schedule {
    quartz_cron_expression = "0 30 3 * * ?"
    timezone_id            = "UTC"
  }
}
