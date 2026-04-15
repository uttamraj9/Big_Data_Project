output "adls_storage_account_name" {
  description = "ADLS Gen2 storage account name"
  value       = module.adls.storage_account_name
}

output "adls_raw_container" {
  description = "ADLS raw container name"
  value       = module.adls.raw_container_name
}

output "adls_curated_container" {
  description = "ADLS curated container name"
  value       = module.adls.curated_container_name
}

output "adls_gold_container" {
  description = "ADLS gold container name"
  value       = module.adls.gold_container_name
}

output "adf_name" {
  description = "Azure Data Factory name"
  value       = module.adf.adf_name
}

output "databricks_workspace_url" {
  description = "Databricks workspace URL"
  value       = module.databricks.workspace_url
}

output "synapse_serverless_endpoint" {
  description = "Synapse serverless SQL endpoint"
  value       = module.synapse.serverless_endpoint
}

output "key_vault_uri" {
  description = "Key Vault URI"
  value       = module.keyvault.key_vault_uri
}
