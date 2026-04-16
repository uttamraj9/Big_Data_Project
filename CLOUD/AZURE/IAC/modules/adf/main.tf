terraform {
  required_providers {
    azurerm = { source = "hashicorp/azurerm" }
    null    = { source = "hashicorp/null" }
    local   = { source = "hashicorp/local" }
  }
}

# ─── Azure Data Factory ───────────────────────────────────────
resource "azurerm_data_factory" "adf" {
  name                = var.adf_name
  resource_group_name = var.resource_group_name
  location            = var.location

  identity {
    type = "SystemAssigned"
  }
}

# ─── Grant ADF Managed Identity "Key Vault Secrets User" (RBAC) ─
resource "azurerm_role_assignment" "adf_kv_secrets_user" {
  scope                = var.key_vault_id
  role_definition_name = "Key Vault Secrets User"
  principal_id         = azurerm_data_factory.adf.identity[0].principal_id
}

# ─── Linked Service: Key Vault ───────────────────────────────
resource "azurerm_data_factory_linked_service_key_vault" "kv_ls" {
  name            = "LS_CC_Fraud_KeyVault"
  data_factory_id = azurerm_data_factory.adf.id
  key_vault_id    = var.key_vault_id
  depends_on      = [azurerm_role_assignment.adf_kv_secrets_user]
}

# ─── Linked Service: PostgreSQL V2 (via AZ CLI — TF provider creates deprecated type) ─
locals {
  pg_ls_props = jsonencode({
    type = "PostgreSqlV2"
    typeProperties = {
      server             = var.pg_host
      port               = tonumber(var.pg_port)
      database           = var.pg_database
      username           = var.pg_username
      sslMode            = 0
      authenticationType = "Basic"
      password = {
        type  = "AzureKeyVaultSecret"
        store = {
          referenceName = "LS_CC_Fraud_KeyVault"
          type          = "LinkedServiceReference"
        }
        secretName = var.pg_password_secret_name
      }
    }
  })
}

resource "local_file" "pg_ls_json" {
  filename = "/tmp/cc_fraud_adf_pg_ls.json"
  content  = local.pg_ls_props
}

resource "null_resource" "pg_ls" {
  triggers = {
    resource_group_name = var.resource_group_name
    adf_name            = var.adf_name
    props_hash          = sha256(local.pg_ls_props)
    kv_ls_id            = azurerm_data_factory_linked_service_key_vault.kv_ls.id
  }

  provisioner "local-exec" {
    command = "az datafactory linked-service create --resource-group '${var.resource_group_name}' --factory-name '${var.adf_name}' --linked-service-name 'LS_CC_Fraud_PostgreSQL' --properties @${local_file.pg_ls_json.filename}"
  }

  provisioner "local-exec" {
    when    = destroy
    command = "az datafactory linked-service delete --resource-group '${self.triggers.resource_group_name}' --factory-name '${self.triggers.adf_name}' --linked-service-name 'LS_CC_Fraud_PostgreSQL' --yes 2>/dev/null || true"
  }

  depends_on = [
    azurerm_data_factory_linked_service_key_vault.kv_ls,
    local_file.pg_ls_json,
  ]
}

# ─── Linked Service: ADLS Gen2 ───────────────────────────────
resource "azurerm_data_factory_linked_service_azure_blob_storage" "adls_ls" {
  name            = "LS_CC_Fraud_ADLS"
  data_factory_id = azurerm_data_factory.adf.id

  connection_string = "DefaultEndpointsProtocol=https;AccountName=${var.adls_account_name};AccountKey=${var.adls_account_key};EndpointSuffix=core.windows.net"
}
