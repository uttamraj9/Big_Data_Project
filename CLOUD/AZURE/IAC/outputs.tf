output "adls_storage_account_name" {
  value = module.adls.storage_account_name
}

output "adls_raw_container" {
  value = module.adls.raw_container_name
}

output "adls_curated_container" {
  value = module.adls.curated_container_name
}

output "adls_gold_container" {
  value = module.adls.gold_container_name
}

output "adf_name" {
  value = module.adf.adf_name
}

output "databricks_workspace_url" {
  value = module.databricks.workspace_url
}

output "synapse_serverless_endpoint" {
  value = module.synapse.serverless_endpoint
}

output "key_vault_uri" {
  value = module.keyvault.key_vault_uri
}
