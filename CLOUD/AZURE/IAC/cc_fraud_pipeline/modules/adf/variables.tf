variable "resource_group_name" {}
variable "adf_name" {}
variable "adls_account_name" {}
variable "adls_account_key" { sensitive = true }
variable "raw_container_name" {}
variable "key_vault_id" {}
variable "key_vault_uri" {}
variable "pg_host" {}
variable "pg_port" {}
variable "pg_database" {}
variable "pg_username" {}
variable "pg_password_secret_name" {}
