terraform {
  required_providers {
    azurerm = { source = "hashicorp/azurerm" }
  }
}

# ─── Key Vault with Azure RBAC authorization ─────────────────
resource "azurerm_key_vault" "kv" {
  name                      = var.key_vault_name
  resource_group_name       = var.resource_group_name
  location                  = var.location
  tenant_id                 = var.tenant_id
  sku_name                  = "standard"
  enable_rbac_authorization = true
  purge_protection_enabled  = false
  soft_delete_retention_days = 7
}

# ─── Grant caller Key Vault Secrets Officer (RBAC mode) ──────
resource "azurerm_role_assignment" "deployer_kv_secrets" {
  scope                = azurerm_key_vault.kv.id
  role_definition_name = "Key Vault Secrets Officer"
  principal_id         = var.current_object_id
}

# ─── Secrets for cc_fraud pipeline ───────────────────────────
resource "azurerm_key_vault_secret" "pg_host" {
  name         = "cc-fraud-pg-host"
  value        = var.pg_host
  key_vault_id = azurerm_key_vault.kv.id
  depends_on   = [azurerm_role_assignment.deployer_kv_secrets]
}

resource "azurerm_key_vault_secret" "pg_port" {
  name         = "cc-fraud-pg-port"
  value        = tostring(var.pg_port)
  key_vault_id = azurerm_key_vault.kv.id
  depends_on   = [azurerm_role_assignment.deployer_kv_secrets]
}

resource "azurerm_key_vault_secret" "pg_database" {
  name         = "cc-fraud-pg-database"
  value        = var.pg_database
  key_vault_id = azurerm_key_vault.kv.id
  depends_on   = [azurerm_role_assignment.deployer_kv_secrets]
}

resource "azurerm_key_vault_secret" "pg_username" {
  name         = "cc-fraud-pg-username"
  value        = var.pg_username
  key_vault_id = azurerm_key_vault.kv.id
  depends_on   = [azurerm_role_assignment.deployer_kv_secrets]
}

resource "azurerm_key_vault_secret" "pg_password" {
  name         = "cc-fraud-pg-password"
  value        = var.pg_password
  key_vault_id = azurerm_key_vault.kv.id
  depends_on   = [azurerm_role_assignment.deployer_kv_secrets]
}

resource "azurerm_key_vault_secret" "adls_account_key" {
  name         = "cc-fraud-adls-account-key"
  value        = var.adls_account_key
  key_vault_id = azurerm_key_vault.kv.id
  depends_on   = [azurerm_role_assignment.deployer_kv_secrets]
}
