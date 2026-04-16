terraform {
  required_providers {
    azurerm = { source = "hashicorp/azurerm" }
    local   = { source = "hashicorp/local" }
  }
}

# ─── Synapse workspace ────────────────────────────────────────
resource "azurerm_synapse_workspace" "synapse" {
  name                                 = var.synapse_workspace_name
  resource_group_name                  = var.resource_group_name
  location                             = var.location
  storage_data_lake_gen2_filesystem_id = var.adls_filesystem_id
  sql_administrator_login              = var.synapse_sql_admin
  sql_administrator_login_password     = var.synapse_sql_password

  identity {
    type = "SystemAssigned"
  }
}

# ─── Allow Azure services to reach Synapse SQL On-Demand ─────
resource "azurerm_synapse_firewall_rule" "allow_azure" {
  name                 = "AllowAllWindowsAzureIps"
  synapse_workspace_id = azurerm_synapse_workspace.synapse.id
  start_ip_address     = "0.0.0.0"
  end_ip_address       = "0.0.0.0"
}

# ─── Grant Synapse Managed Identity read access to ADLS ──────
resource "azurerm_role_assignment" "synapse_adls_access" {
  scope                = var.adls_account_id
  role_definition_name = "Storage Blob Data Reader"
  principal_id         = azurerm_synapse_workspace.synapse.identity[0].principal_id
}

