# ─── ADLS Gen2 Storage Account ──────────────────────────────
resource "azurerm_storage_account" "adls" {
  name                     = "${replace(var.project, "-", "")}${var.environment}adls"
  resource_group_name      = var.resource_group_name
  location                 = var.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true # Hierarchical Namespace = ADLS Gen2

  blob_properties {
    versioning_enabled = true
    delete_retention_policy {
      days = 7
    }
  }

  tags = {
    project     = var.project
    environment = var.environment
    layer       = "storage"
  }
}

# ─── Containers (Layers) ─────────────────────────────────────
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
