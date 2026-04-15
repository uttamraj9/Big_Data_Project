# ─── Azure Data Factory ──────────────────────────────────────
resource "azurerm_data_factory" "adf" {
  name                = "${var.project}-${var.environment}-adf"
  resource_group_name = var.resource_group_name
  location            = var.location

  identity {
    type = "SystemAssigned"
  }

  tags = {
    project     = var.project
    environment = var.environment
    layer       = "ingestion"
  }
}

# ─── Grant ADF access to Key Vault ──────────────────────────
resource "azurerm_key_vault_access_policy" "adf_kv_policy" {
  key_vault_id = var.key_vault_id
  tenant_id    = azurerm_data_factory.adf.identity[0].tenant_id
  object_id    = azurerm_data_factory.adf.identity[0].principal_id

  secret_permissions = ["Get", "List"]
}

# ─── Linked Service: Key Vault ───────────────────────────────
resource "azurerm_data_factory_linked_service_key_vault" "kv_ls" {
  name            = "LS_KeyVault"
  data_factory_id = azurerm_data_factory.adf.id
  key_vault_id    = var.key_vault_id

  depends_on = [azurerm_key_vault_access_policy.adf_kv_policy]
}

# ─── Linked Service: PostgreSQL ──────────────────────────────
resource "azurerm_data_factory_linked_service_postgresql" "pg_ls" {
  name            = "LS_PostgreSQL_Source"
  data_factory_id = azurerm_data_factory.adf.id

  connection_string = "Host=${var.pg_host};Port=${var.pg_port};Database=${var.pg_database};UID=${var.pg_username};EncryptionMethod=0"

  parameters = {}

  additional_properties = {
    "Password" = jsonencode({
      "type"      = "AzureKeyVaultSecret"
      "store"     = { "referenceName" = "LS_KeyVault", "type" = "LinkedServiceReference" }
      "secretName" = var.pg_password_secret_name
    })
  }

  depends_on = [azurerm_data_factory_linked_service_key_vault.kv_ls]
}

# ─── Linked Service: ADLS Gen2 ───────────────────────────────
resource "azurerm_data_factory_linked_service_azure_blob_storage" "adls_ls" {
  name            = "LS_ADLS_Gen2"
  data_factory_id = azurerm_data_factory.adf.id

  connection_string = "DefaultEndpointsProtocol=https;AccountName=${var.adls_account_name};AccountKey=${var.adls_account_key};EndpointSuffix=core.windows.net"
}

# ─── Dataset: PostgreSQL Source (Employees table) ────────────
resource "azurerm_data_factory_dataset_postgresql" "pg_employees_ds" {
  name                = "DS_PG_Employees"
  data_factory_id     = azurerm_data_factory.adf.id
  linked_service_name = azurerm_data_factory_linked_service_postgresql.pg_ls.name

  table_name = "employees"

  depends_on = [azurerm_data_factory_linked_service_postgresql.pg_ls]
}

# ─── Dataset: ADLS Raw Destination ───────────────────────────
resource "azurerm_data_factory_dataset_delimited_text" "adls_raw_ds" {
  name                = "DS_ADLS_Raw_Employees"
  data_factory_id     = azurerm_data_factory.adf.id
  linked_service_name = azurerm_data_factory_linked_service_azure_blob_storage.adls_ls.name

  azure_blob_storage_location {
    container = var.raw_container_name
    path      = "employees"
    filename  = "employees.csv"
  }

  column_delimiter    = ","
  row_delimiter       = "\n"
  first_row_as_header = true
  quote_character     = "\""

  depends_on = [azurerm_data_factory_linked_service_azure_blob_storage.adls_ls]
}

# ─── Pipeline: PostgreSQL → ADLS Raw ─────────────────────────
resource "azurerm_data_factory_pipeline" "pg_to_raw" {
  name            = "PL_PostgreSQL_To_Raw"
  data_factory_id = azurerm_data_factory.adf.id
  description     = "Ingests data from PostgreSQL source to ADLS raw layer"

  activities_json = jsonencode([
    {
      name = "Copy_Employees_To_Raw"
      type = "Copy"
      inputs = [
        {
          referenceName = "DS_PG_Employees"
          type          = "DatasetReference"
        }
      ]
      outputs = [
        {
          referenceName = "DS_ADLS_Raw_Employees"
          type          = "DatasetReference"
        }
      ]
      typeProperties = {
        source = {
          type            = "PostgreSqlSource"
          query           = "SELECT * FROM employees"
          queryTimeout    = "02:00:00"
        }
        sink = {
          type                  = "DelimitedTextSink"
          storeSettings = {
            type      = "AzureBlobStorageWriteSettings"
            copyBehavior = "PreserveHierarchy"
          }
          formatSettings = {
            type             = "DelimitedTextWriteSettings"
            quoteAllText     = false
            fileExtension    = ".csv"
          }
        }
        enableStaging = false
        translator = {
          type           = "TabularTranslator"
          typeConversion = true
          typeConversionSettings = {
            allowDataTruncation  = true
            treatBooleanAsNumber = false
          }
        }
      }
    }
  ])

  depends_on = [
    azurerm_data_factory_dataset_postgresql.pg_employees_ds,
    azurerm_data_factory_dataset_delimited_text.adls_raw_ds
  ]
}

# ─── Trigger: Daily schedule ─────────────────────────────────
resource "azurerm_data_factory_trigger_schedule" "daily_trigger" {
  name            = "TR_Daily_Ingest"
  data_factory_id = azurerm_data_factory.adf.id
  pipeline_name   = azurerm_data_factory_pipeline.pg_to_raw.name

  interval  = 1
  frequency = "Day"
  start_time = "2024-01-01T02:00:00Z"

  activated = true

  depends_on = [azurerm_data_factory_pipeline.pg_to_raw]
}
