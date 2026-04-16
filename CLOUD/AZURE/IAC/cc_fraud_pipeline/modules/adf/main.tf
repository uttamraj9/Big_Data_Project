terraform {
  required_providers {
    azurerm = { source = "hashicorp/azurerm" }
    null    = { source = "hashicorp/null" }
    local   = { source = "hashicorp/local" }
  }
}

# ─── Reference existing ADF ──────────────────────────────────
data "azurerm_data_factory" "adf" {
  name                = var.adf_name
  resource_group_name = var.resource_group_name
}

# ─── Grant ADF Managed Identity "Key Vault Secrets User" (RBAC mode) ─
resource "azurerm_role_assignment" "adf_kv_secrets_user" {
  scope                = var.key_vault_id
  role_definition_name = "Key Vault Secrets User"
  principal_id         = data.azurerm_data_factory.adf.identity[0].principal_id
}

# ─── Linked Service: Key Vault ───────────────────────────────
resource "azurerm_data_factory_linked_service_key_vault" "kv_ls" {
  name            = "LS_CC_Fraud_KeyVault"
  data_factory_id = data.azurerm_data_factory.adf.id
  key_vault_id    = var.key_vault_id
  depends_on      = [azurerm_role_assignment.adf_kv_secrets_user]
}

# ─── PostgreSQL V2 LS + Dataset via AZ CLI ───────────────────
# azurerm_data_factory_linked_service_postgresql creates the deprecated
# AzurePostgreSql type; ADF now requires PostgreSqlV2, so we use az CLI.
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

  pg_ds_props = jsonencode({
    type = "PostgreSqlV2Table"
    linkedServiceName = {
      referenceName = "LS_CC_Fraud_PostgreSQL"
      type          = "LinkedServiceReference"
    }
    typeProperties = {
      schema = "public"
      table  = "cc_fraud_trans"
    }
  })
}

resource "local_file" "pg_ls_json" {
  filename = "/tmp/cc_fraud_adf_pg_ls.json"
  content  = local.pg_ls_props
}

resource "local_file" "pg_ds_json" {
  filename = "/tmp/cc_fraud_adf_pg_ds.json"
  content  = local.pg_ds_props
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

resource "null_resource" "pg_fraud_ds" {
  triggers = {
    resource_group_name = var.resource_group_name
    adf_name            = var.adf_name
    pg_ls_id            = null_resource.pg_ls.id
    props_hash          = sha256(local.pg_ds_props)
  }

  provisioner "local-exec" {
    command = "az datafactory dataset create --resource-group '${var.resource_group_name}' --factory-name '${var.adf_name}' --dataset-name 'DS_CC_Fraud_Trans_PG' --properties @${local_file.pg_ds_json.filename}"
  }

  provisioner "local-exec" {
    when    = destroy
    command = "az datafactory dataset delete --resource-group '${self.triggers.resource_group_name}' --factory-name '${self.triggers.adf_name}' --dataset-name 'DS_CC_Fraud_Trans_PG' --yes 2>/dev/null || true"
  }

  depends_on = [
    null_resource.pg_ls,
    local_file.pg_ds_json,
  ]
}

# ─── Linked Service: ADLS Gen2 ───────────────────────────────
resource "azurerm_data_factory_linked_service_azure_blob_storage" "adls_ls" {
  name            = "LS_CC_Fraud_ADLS"
  data_factory_id = data.azurerm_data_factory.adf.id

  connection_string = "DefaultEndpointsProtocol=https;AccountName=${var.adls_account_name};AccountKey=${var.adls_account_key};EndpointSuffix=core.windows.net"
}

# ─── Dataset: ADLS raw destination ───────────────────────────
resource "azurerm_data_factory_dataset_delimited_text" "adls_raw_ds" {
  name                = "DS_CC_Fraud_Trans_ADLS_Raw"
  data_factory_id     = data.azurerm_data_factory.adf.id
  linked_service_name = azurerm_data_factory_linked_service_azure_blob_storage.adls_ls.name

  azure_blob_storage_location {
    container = var.raw_container_name
    path      = "cc_fraud_trans"
    filename  = "cc_fraud_trans.csv"
  }

  column_delimiter    = ","
  row_delimiter       = "\n"
  first_row_as_header = true
  quote_character     = "\""

  depends_on = [azurerm_data_factory_linked_service_azure_blob_storage.adls_ls]
}

# ─── Pipeline: PostgreSQL → ADLS raw ─────────────────────────
resource "azurerm_data_factory_pipeline" "pg_to_raw" {
  name            = "PL_CC_Fraud_Trans_PG_To_Raw"
  data_factory_id = data.azurerm_data_factory.adf.id
  description     = "Copies cc_fraud_trans from ON_PREM PostgreSQL (${var.pg_host}) to ADLS Gen2 raw layer"

  activities_json = jsonencode([
    {
      name    = "Copy_CC_Fraud_Trans"
      type    = "Copy"
      inputs  = [{ referenceName = "DS_CC_Fraud_Trans_PG",       type = "DatasetReference" }]
      outputs = [{ referenceName = "DS_CC_Fraud_Trans_ADLS_Raw", type = "DatasetReference" }]
      typeProperties = {
        source = {
          type         = "PostgreSqlV2Source"
          query        = "SELECT * FROM cc_fraud_trans"
          queryTimeout = "02:00:00"
        }
        sink = {
          type           = "DelimitedTextSink"
          storeSettings  = { type = "AzureBlobStorageWriteSettings", copyBehavior = "PreserveHierarchy" }
          formatSettings = { type = "DelimitedTextWriteSettings", quoteAllText = false, fileExtension = ".csv" }
        }
        enableStaging = false
        translator = {
          type                    = "TabularTranslator"
          typeConversion          = true
          typeConversionSettings  = { allowDataTruncation = true, treatBooleanAsNumber = false }
        }
      }
    }
  ])

  depends_on = [
    null_resource.pg_fraud_ds,
    azurerm_data_factory_dataset_delimited_text.adls_raw_ds,
  ]
}

# ─── Trigger: Daily at 01:00 UTC ─────────────────────────────
resource "azurerm_data_factory_trigger_schedule" "daily_trigger" {
  name            = "TR_CC_Fraud_Daily_Ingest"
  data_factory_id = data.azurerm_data_factory.adf.id
  pipeline_name   = azurerm_data_factory_pipeline.pg_to_raw.name

  interval   = 1
  frequency  = "Day"
  start_time = "2024-01-01T01:00:00Z"
  activated  = true

  depends_on = [azurerm_data_factory_pipeline.pg_to_raw]
}
