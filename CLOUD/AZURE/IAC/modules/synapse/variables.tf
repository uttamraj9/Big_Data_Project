variable "resource_group_name" {}
variable "location" {}
variable "synapse_workspace_name" {}
variable "adls_account_name" {}
variable "adls_account_id" {}
variable "adls_filesystem_id" {}
variable "synapse_sql_admin" {}
variable "synapse_sql_password" { sensitive = true }
