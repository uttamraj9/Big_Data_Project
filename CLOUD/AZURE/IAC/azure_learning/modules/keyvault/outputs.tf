output "key_vault_id" {
  value = azurerm_key_vault.kv.id
}

output "key_vault_uri" {
  value = azurerm_key_vault.kv.vault_uri
}

output "pg_password_secret_name" {
  value = azurerm_key_vault_secret.pg_password.name
}

output "adls_key_secret_name" {
  value = azurerm_key_vault_secret.adls_account_key.name
}
