variable "project" {}
variable "environment" {}
variable "vpc_id" {}
variable "subnet_ids" { type = list(string) }
variable "source_db_host" {}
variable "source_db_port" { type = number }
variable "source_db_name" {}
variable "source_db_user" {}
variable "source_db_password" { sensitive = true }
variable "bronze_bucket_name" {}
variable "dms_role_arn" {}
