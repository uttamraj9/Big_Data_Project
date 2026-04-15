# ─── Key Vault ───────────────────────────────────────────────
resource "azurerm_key_vault" "kv" {
  name                       = "${var.project}-${var.environment}-kv"
  resource_group_name        = var.resource_group_name
  location                   = var.location
  tenant_id                  = var.tenant_id
  sku_name                   = "standard"
  soft_delete_retention_days = 7
  purge_protection_enabled   = false

  access_policy {
    tenant_id = var.tenant_id
    object_id = var.current_object_id

    secret_permissions = ["Get", "Set", "List", "Delete", "Purge"]
  }

  tags = {
    project     = var.project
    environment = var.environment
  }
}

# ─── Secrets ─────────────────────────────────────────────────
resource "azurerm_key_vault_secret" "pg_host" {
  name         = "pg-host"
  value        = var.pg_host
  key_vault_id = azurerm_key_vault.kv.id
}

resource "azurerm_key_vault_secret" "pg_port" {
  name         = "pg-port"
  value        = tostring(var.pg_port)
  key_vault_id = azurerm_key_vault.kv.id
}

resource "azurerm_key_vault_secret" "pg_database" {
  name         = "pg-database"
  value        = var.pg_database
  key_vault_id = azurerm_key_vault.kv.id
}

resource "azurerm_key_vault_secret" "pg_username" {
  name         = "pg-username"
  value        = var.pg_username
  key_vault_id = azurerm_key_vault.kv.id
}

resource "azurerm_key_vault_secret" "pg_password" {
  name         = "pg-password"
  value        = var.pg_password
  key_vault_id = azurerm_key_vault.kv.id
}

resource "azurerm_key_vault_secret" "adls_account_key" {
  name         = "adls-account-key"
  value        = var.adls_account_key
  key_vault_id = azurerm_key_vault.kv.id
}
