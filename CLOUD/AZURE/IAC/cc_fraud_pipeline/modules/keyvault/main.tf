# ─── Reference existing Key Vault ────────────────────────────
data "azurerm_key_vault" "kv" {
  name                = var.key_vault_name
  resource_group_name = var.resource_group_name
}

# ─── Grant current caller access to set secrets ──────────────
resource "azurerm_key_vault_access_policy" "deployer" {
  key_vault_id = data.azurerm_key_vault.kv.id
  tenant_id    = var.tenant_id
  object_id    = var.current_object_id

  secret_permissions = ["Get", "Set", "List", "Delete", "Purge"]
}

# ─── Secrets for cc_fraud pipeline ───────────────────────────
resource "azurerm_key_vault_secret" "pg_host" {
  name         = "cc-fraud-pg-host"
  value        = var.pg_host
  key_vault_id = data.azurerm_key_vault.kv.id
  depends_on   = [azurerm_key_vault_access_policy.deployer]
}

resource "azurerm_key_vault_secret" "pg_port" {
  name         = "cc-fraud-pg-port"
  value        = tostring(var.pg_port)
  key_vault_id = data.azurerm_key_vault.kv.id
  depends_on   = [azurerm_key_vault_access_policy.deployer]
}

resource "azurerm_key_vault_secret" "pg_database" {
  name         = "cc-fraud-pg-database"
  value        = var.pg_database
  key_vault_id = data.azurerm_key_vault.kv.id
  depends_on   = [azurerm_key_vault_access_policy.deployer]
}

resource "azurerm_key_vault_secret" "pg_username" {
  name         = "cc-fraud-pg-username"
  value        = var.pg_username
  key_vault_id = data.azurerm_key_vault.kv.id
  depends_on   = [azurerm_key_vault_access_policy.deployer]
}

resource "azurerm_key_vault_secret" "pg_password" {
  name         = "cc-fraud-pg-password"
  value        = var.pg_password
  key_vault_id = data.azurerm_key_vault.kv.id
  depends_on   = [azurerm_key_vault_access_policy.deployer]
}

resource "azurerm_key_vault_secret" "adls_account_key" {
  name         = "cc-fraud-adls-account-key"
  value        = var.adls_account_key
  key_vault_id = data.azurerm_key_vault.kv.id
  depends_on   = [azurerm_key_vault_access_policy.deployer]
}
