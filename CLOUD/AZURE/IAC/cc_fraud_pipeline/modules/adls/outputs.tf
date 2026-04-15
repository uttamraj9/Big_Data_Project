output "storage_account_name" {
  value = data.azurerm_storage_account.adls.name
}

output "storage_account_id" {
  value = data.azurerm_storage_account.adls.id
}

output "storage_account_key" {
  value     = data.azurerm_storage_account.adls.primary_access_key
  sensitive = true
}

output "dfs_endpoint" {
  value = data.azurerm_storage_account.adls.primary_dfs_endpoint
}

output "raw_container_name" {
  value = azurerm_storage_container.raw.name
}

output "curated_container_name" {
  value = azurerm_storage_container.curated.name
}

output "gold_container_name" {
  value = azurerm_storage_container.gold.name
}
