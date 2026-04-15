variable "project" {}
variable "environment" {}
variable "vpc_id" {}
variable "subnet_ids" { type = list(string) }
variable "silver_bucket_name" {}
variable "redshift_role_arn" {}
variable "db_password" { sensitive = true }
