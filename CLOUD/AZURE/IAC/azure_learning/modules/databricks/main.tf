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
  cluster_name            = "${var.project}-transform-cluster"
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
  }

  library {
    pypi {
      package = "psycopg2-binary"
    }
  }

  depends_on = [azurerm_databricks_workspace.dbw]
}

# ─── Databricks Secret Scope (backed by Azure Key Vault) ─────
resource "databricks_secret_scope" "adls_scope" {
  name = "adls-scope"

  keyvault_metadata {
    resource_id = ""  # populated at runtime via CLI or separate config
    dns_name    = ""
  }

  depends_on = [azurerm_databricks_workspace.dbw]
}

# ─── Databricks Notebook: Raw → Curated Transform ────────────
resource "databricks_notebook" "transform" {
  path     = "/Shared/itc_pipeline/raw_to_curated"
  language = "PYTHON"
  source   = "${path.module}/../../scripts/databricks/transform.py"

  depends_on = [azurerm_databricks_workspace.dbw]
}

# ─── Databricks Job: Schedule transform notebook ─────────────
resource "databricks_job" "transform_job" {
  name = "${var.project}-raw-to-curated"

  task {
    task_key = "raw_to_curated"

    notebook_task {
      notebook_path = databricks_notebook.transform.path
      base_parameters = {
        adls_account_name  = var.adls_account_name
        raw_container      = var.raw_container_name
        curated_container  = var.curated_container_name
        table_name         = "employees"
      }
    }

    existing_cluster_id = databricks_cluster.transform_cluster.id
  }

  schedule {
    quartz_cron_expression = "0 30 3 * * ?"  # 03:30 AM daily (after ADF ingest)
    timezone_id            = "UTC"
  }

  depends_on = [
    databricks_cluster.transform_cluster,
    databricks_notebook.transform
  ]
}
