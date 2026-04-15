variable "subscription_id" {
  description = "Azure Subscription ID"
  default     = "3a72be92-287b-4f1e-840a-5e3e71100139"
}

variable "tenant_id" {
  description = "Azure Tenant ID"
  default     = "2b32b1fa-7899-482e-a6de-be99c0ff5516"
}

variable "resource_group_name" {
  description = "Existing resource group to deploy into"
  default     = "Itc_Bigdata"
}

variable "project" {
  description = "Project name prefix for all resources"
  default     = "itc-bd"
}

variable "environment" {
  description = "Environment (dev/staging/prod)"
  default     = "dev"
}

# ─── PostgreSQL Source ───────────────────────────────────────
variable "pg_host" {
  description = "PostgreSQL host IP"
  default     = "13.42.152.118"
}

variable "pg_port" {
  description = "PostgreSQL port"
  default     = 5432
}

variable "pg_database" {
  description = "PostgreSQL database name"
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

# ─── Existing Synapse Workspace ──────────────────────────────
variable "existing_synapse_workspace" {
  description = "Name of the existing Synapse workspace to use for gold layer"
  default     = "itc-bd-ne-synapse"
}
