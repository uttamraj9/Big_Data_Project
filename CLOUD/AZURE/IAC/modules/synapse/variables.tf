variable "resource_group_name" {}
variable "location" {}
variable "synapse_workspace_name" {}
variable "adls_account_id" {}
variable "adls_filesystem_id" {}
variable "synapse_sql_admin" {}
variable "synapse_sql_password" { sensitive = true }

variable "studio_access_groups" {
  description = "Map of AAD group display_name => object_id to grant Synapse Contributor + ADLS Reader. Add a new entry here to onboard another group."
  type        = map(string)
  default     = {}
}
