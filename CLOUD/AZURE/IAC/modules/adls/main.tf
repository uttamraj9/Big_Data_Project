terraform {
  required_providers {
    azurerm = { source = "hashicorp/azurerm" }
  }
}

# ─── ADLS Gen2 storage account ───────────────────────────────
resource "azurerm_storage_account" "adls" {
  name                     = var.adls_account_name
  resource_group_name      = var.resource_group_name
  location                 = var.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true   # required for ADLS Gen2 / ABFSS
}

# ─── Filesystem for Synapse primary storage (ABFSS format) ───
resource "azurerm_storage_data_lake_gen2_filesystem" "synapsefs" {
  name               = "synapsefs"
  storage_account_id = azurerm_storage_account.adls.id
}

# ─── cc_fraud pipeline containers ────────────────────────────
resource "azurerm_storage_container" "raw" {
  name                  = "raw"
  storage_account_name  = azurerm_storage_account.adls.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "curated" {
  name                  = "curated"
  storage_account_name  = azurerm_storage_account.adls.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "gold" {
  name                  = "gold"
  storage_account_name  = azurerm_storage_account.adls.name
  container_access_type = "private"
}
