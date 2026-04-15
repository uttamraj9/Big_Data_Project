# ─── Reference existing ADLS Gen2 account ────────────────────
data "azurerm_storage_account" "adls" {
  name                = var.adls_account_name
  resource_group_name = var.resource_group_name
}

# ─── Create cc_fraud containers if they don't exist ──────────
# 'raw' already exists in itcbdneadls; curated and gold are new.

resource "azurerm_storage_container" "raw" {
  name                  = "raw"
  storage_account_name  = data.azurerm_storage_account.adls.name
  container_access_type = "private"

  lifecycle {
    ignore_changes = [name]  # container already exists
  }
}

resource "azurerm_storage_container" "curated" {
  name                  = "curated"
  storage_account_name  = data.azurerm_storage_account.adls.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "gold" {
  name                  = "gold"
  storage_account_name  = data.azurerm_storage_account.adls.name
  container_access_type = "private"
}
