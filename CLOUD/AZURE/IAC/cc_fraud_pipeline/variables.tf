variable "subscription_id" {
  description = "Azure Subscription ID"
  default     = "3a72be92-287b-4f1e-840a-5e3e71100139"
}

variable "tenant_id" {
  description = "Azure Tenant ID"
  default     = "2b32b1fa-7899-482e-a6de-be99c0ff5516"
}

variable "resource_group_name" {
  description = "Existing resource group"
  default     = "Itc_Bigdata"
}

# ─── Existing resource names ─────────────────────────────────
variable "adls_account_name" {
  description = "Existing ADLS Gen2 storage account"
  default     = "itcbdneadls"
}

variable "adf_name" {
  description = "Existing Azure Data Factory"
  default     = "itc-bd-ne-adf"
}

variable "key_vault_name" {
  description = "Existing Key Vault"
  default     = "itc-bd-ne-kv"
}

variable "databricks_workspace_name" {
  description = "Existing Databricks workspace"
  default     = "itc-bd-ne-adb"
}

variable "databricks_workspace_resource_id" {
  description = "Full resource ID of existing Databricks workspace"
  default     = "/subscriptions/3a72be92-287b-4f1e-840a-5e3e71100139/resourceGroups/Itc_Bigdata/providers/Microsoft.Databricks/workspaces/itc-bd-ne-adb"
}

variable "databricks_workspace_host" {
  description = "Databricks workspace host URL (without https://)"
  default     = "adb-7405604468967976.16.azuredatabricks.net"
}

variable "synapse_workspace_name" {
  description = "Existing Synapse workspace"
  default     = "itc-bd-ne-synapse"
}

# ─── PostgreSQL Source (ON_PREM server) ─────────────────────
variable "pg_host" {
  description = "PostgreSQL host — ON_PREM server"
  default     = "13.42.152.118"
}

variable "pg_port" {
  description = "PostgreSQL port"
  default     = 5432
}

variable "pg_database" {
  description = "PostgreSQL database"
  default     = "testdb"
}

variable "pg_username" {
  description = "PostgreSQL username"
  default     = "admin"
}

variable "pg_password" {
  description = "PostgreSQL password"
  sensitive   = true
  default     = "admin123"
}
