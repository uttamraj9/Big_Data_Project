variable "resource_group_name" {}
variable "location" {}
variable "adf_name" {}
variable "adls_account_name" {}
variable "adls_account_key" { sensitive = true }
variable "key_vault_id" {}
variable "pg_host" {}
variable "pg_port" {}
variable "pg_database" {}
variable "pg_username" {}
variable "pg_password_secret_name" {}
