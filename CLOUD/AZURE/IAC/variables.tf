variable "subscription_id" {
  description = "Azure Subscription ID"
  default     = "3a72be92-287b-4f1e-840a-5e3e71100139"
}

variable "tenant_id" {
  description = "Azure Tenant ID"
  default     = "2b32b1fa-7899-482e-a6de-be99c0ff5516"
}

variable "location" {
  description = "Azure region"
  default     = "northeurope"
}

variable "resource_group_name" {
  description = "Resource group to create"
  default     = "Itc_Bigdata"
}

# ─── Resource names ──────────────────────────────────────────
variable "adls_account_name" {
  description = "ADLS Gen2 storage account name"
  default     = "itcbdneadls"
}

variable "adf_name" {
  description = "Azure Data Factory name"
  default     = "itc-bd-ne-adf"
}

variable "key_vault_name" {
  description = "Key Vault name"
  default     = "itc-bd-ne-kv"
}

variable "databricks_workspace_name" {
  description = "Databricks workspace name"
  default     = "itc-bd-ne-adb"
}

variable "synapse_workspace_name" {
  description = "Synapse workspace name"
  default     = "itc-bd-ne-synapse"
}

# ─── Synapse SQL admin ────────────────────────────────────────
variable "synapse_sql_admin" {
  description = "Synapse SQL administrator login"
  default     = "sqladmin"
}

variable "synapse_sql_password" {
  description = "Synapse SQL administrator password"
  sensitive   = true
}

# ─── PostgreSQL Source (ON_PREM server) ──────────────────────
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

# ─── Synapse Studio access groups ────────────────────────────
variable "studio_access_groups" {
  description = "Map of AAD group display_name => object_id granted Synapse Contributor + ADLS Reader. Add a new entry to onboard another group — no module changes needed."
  type        = map(string)
  default     = {}
}

variable "kv_secrets_officer_groups" {
  description = "Map of AAD group display_name => object_id granted Key Vault Secrets Officer. Add a new entry to allow another group to create/manage secrets."
  type        = map(string)
  default     = {}
}
