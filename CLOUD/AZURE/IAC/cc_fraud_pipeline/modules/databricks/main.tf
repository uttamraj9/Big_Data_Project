# ─── Azure Databricks Workspace ──────────────────────────────
resource "azurerm_databricks_workspace" "dbw" {
  name                = "${var.project}-${var.environment}-dbw"
  resource_group_name = var.resource_group_name
  location            = var.location
  sku                 = "standard"

  tags = {
    project     = var.project
    environment = var.environment
    layer       = "transformation"
  }
}

# ─── Databricks Cluster ───────────────────────────────────────
resource "databricks_cluster" "transform_cluster" {
  cluster_name            = "${var.project}-fraud-transform"
  spark_version           = "13.3.x-scala2.12"
  node_type_id            = "Standard_DS3_v2"
  autotermination_minutes = 20
  num_workers             = 1

  spark_conf = {
    "spark.databricks.delta.preview.enabled" = "true"
  }

  spark_env_vars = {
    "ADLS_ACCOUNT_NAME" = var.adls_account_name
    "RAW_CONTAINER"     = var.raw_container_name
    "CURATED_CONTAINER" = var.curated_container_name
    "GOLD_CONTAINER"    = var.gold_container_name
  }

  depends_on = [azurerm_databricks_workspace.dbw]
}

# ─── Databricks Secret Scope (backed by Azure Key Vault) ─────
resource "databricks_secret_scope" "adls_scope" {
  name = "adls-scope"

  keyvault_metadata {
    resource_id = ""  # set via CLI after workspace creation
    dns_name    = ""
  }

  depends_on = [azurerm_databricks_workspace.dbw]
}

# ─── Notebooks: uploaded from DATA_PIPELINE/ by Jenkinsfile ──
# The Jenkinsfile in DATA_PIPELINE/cc_fraud_pipeline/ uploads
# the PySpark notebooks via Databricks CLI after terraform apply.
# Jobs below reference the uploaded notebook paths.

# ─── Job: Raw → Curated transform ────────────────────────────
resource "databricks_job" "raw_to_curated" {
  name = "${var.project}-raw-to-curated"

  task {
    task_key = "raw_to_curated"

    notebook_task {
      notebook_path = "/Shared/cc_fraud_pipeline/raw_to_curated"
      base_parameters = {
        adls_account_name  = var.adls_account_name
        raw_container      = var.raw_container_name
        curated_container  = var.curated_container_name
        table_name         = "cc_fraud_trans"
      }
    }

    existing_cluster_id = databricks_cluster.transform_cluster.id
  }

  schedule {
    quartz_cron_expression = "0 30 2 * * ?"  # 02:30 UTC daily (after ADF ingest at 01:00)
    timezone_id            = "UTC"
  }

  depends_on = [databricks_cluster.transform_cluster]
}

# ─── Job: Curated → Gold aggregations ────────────────────────
resource "databricks_job" "curated_to_gold" {
  name = "${var.project}-curated-to-gold"

  task {
    task_key = "curated_to_gold"

    notebook_task {
      notebook_path = "/Shared/cc_fraud_pipeline/curated_to_gold"
      base_parameters = {
        adls_account_name  = var.adls_account_name
        curated_container  = var.curated_container_name
        gold_container     = var.gold_container_name
        table_name         = "cc_fraud_trans"
      }
    }

    existing_cluster_id = databricks_cluster.transform_cluster.id
  }

  schedule {
    quartz_cron_expression = "0 30 3 * * ?"  # 03:30 UTC daily (after raw→curated at 02:30)
    timezone_id            = "UTC"
  }

  depends_on = [databricks_cluster.transform_cluster]
}
