variable "resource_group_name" {}
variable "location" {}
variable "project" {}
variable "environment" {}
variable "adls_account_name" {}
variable "adls_account_key" { sensitive = true }
variable "raw_container_name" {}
variable "curated_container_name" {}
