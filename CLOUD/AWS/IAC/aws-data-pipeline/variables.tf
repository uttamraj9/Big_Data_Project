variable "aws_region" {
  default = "us-east-1"
}

variable "project" {
  default = "uttam-pipeline"
}

variable "environment" {
  default = "dev"
}

variable "vpc_id" {
  default = "vpc-0502575a3758399d9"
}

variable "subnet_ids" {
  default = ["subnet-00f8d487683f62143", "subnet-0c053a166dee1db59"]
}

# ─── PostgreSQL Source DB ───────────────
variable "source_db_host" {
  default = "13.42.152.118"
}

variable "source_db_port" {
  default = 5432
}

variable "source_db_name" {
  default = "testdb"
}

variable "source_db_user" {
  default = "admin"
}

variable "source_db_password" {
  description = "PostgreSQL source password"
  sensitive   = true
  default     = "admin123"
}

# ─── Redshift ───────────────────────────
variable "redshift_password" {
  description = "Redshift master password"
  sensitive   = true
  default     = "Uttam1Redshift99"
}
