variable "resource_group_name" {}
variable "location" {}
variable "key_vault_name" {}
variable "tenant_id" {}
variable "current_object_id" {}
variable "pg_host" {}
variable "pg_port" {}
variable "pg_database" {}
variable "pg_username" {}
variable "pg_password" { sensitive = true }
variable "adls_account_key" { sensitive = true }

variable "kv_secrets_officer_groups" {
  description = "Map of AAD group display_name => object_id granted Key Vault Secrets Officer. Add a new entry to allow another group to create/manage secrets."
  type        = map(string)
  default     = {}
}
