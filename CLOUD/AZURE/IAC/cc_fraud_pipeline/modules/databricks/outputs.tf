output "workspace_url" {
  value = data.azurerm_databricks_workspace.dbw.workspace_url
}

output "raw_to_curated_job_id" {
  value = databricks_job.raw_to_curated.id
}

output "curated_to_gold_job_id" {
  value = databricks_job.curated_to_gold.id
}
