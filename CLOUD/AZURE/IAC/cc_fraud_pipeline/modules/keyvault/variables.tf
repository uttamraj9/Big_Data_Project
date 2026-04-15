variable "resource_group_name" {}
variable "key_vault_name" {}
variable "tenant_id" {}
variable "current_object_id" {}
variable "pg_host" {}
variable "pg_port" {}
variable "pg_database" {}
variable "pg_username" {}
variable "pg_password" { sensitive = true }
variable "adls_account_key" { sensitive = true }
