terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  required_version = ">= 1.3.0"
}

provider "aws" {
  region = var.aws_region
}

# ─────────────────────────────────────────
# S3 – Bronze & Silver layers
# ─────────────────────────────────────────
module "s3" {
  source      = "./modules/s3"
  project     = var.project
  environment = var.environment
}

# ─────────────────────────────────────────
# DMS – PostgreSQL → S3 Bronze
# ─────────────────────────────────────────
module "dms" {
  source              = "./modules/dms"
  project             = var.project
  environment         = var.environment
  source_db_host      = var.source_db_host
  source_db_port      = var.source_db_port
  source_db_name      = var.source_db_name
  source_db_user      = var.source_db_user
  source_db_password  = var.source_db_password
  bronze_bucket_name  = module.s3.bronze_bucket_name
  dms_role_arn        = module.iam.dms_role_arn
  subnet_ids          = var.subnet_ids
  vpc_id              = var.vpc_id
}

# ─────────────────────────────────────────
# Glue – Bronze → transform → Silver
# ─────────────────────────────────────────
module "glue" {
  source             = "./modules/glue"
  project            = var.project
  environment        = var.environment
  bronze_bucket_name = module.s3.bronze_bucket_name
  silver_bucket_name = module.s3.silver_bucket_name
  glue_role_arn      = module.iam.glue_role_arn
  scripts_bucket     = module.s3.scripts_bucket_name
}

# ─────────────────────────────────────────
# Redshift – Silver → Redshift via COPY
# ─────────────────────────────────────────
module "redshift" {
  source             = "./modules/redshift"
  project            = var.project
  environment        = var.environment
  silver_bucket_name = module.s3.silver_bucket_name
  redshift_role_arn  = module.iam.redshift_role_arn
  subnet_ids         = var.subnet_ids
  vpc_id             = var.vpc_id
  db_password        = var.redshift_password
}

# ─────────────────────────────────────────
# Lambda – Schema Evaluation on S3 events
# ─────────────────────────────────────────
module "lambda" {
  source             = "./modules/lambda"
  project            = var.project
  environment        = var.environment
  bronze_bucket_name = module.s3.bronze_bucket_name
  silver_bucket_name = module.s3.silver_bucket_name
  lambda_role_arn    = module.iam.lambda_role_arn
}

# ─────────────────────────────────────────
# Athena – Query S3 Silver layer
# ─────────────────────────────────────────
module "athena" {
  source             = "./modules/athena"
  project            = var.project
  environment        = var.environment
  silver_bucket_name = module.s3.silver_bucket_name
  results_bucket     = module.s3.athena_results_bucket_name
}

# ─────────────────────────────────────────
# CodePipeline – CI/CD for Glue scripts
# ─────────────────────────────────────────
module "codepipeline" {
  source            = "./modules/codepipeline"
  project           = var.project
  environment       = var.environment
  scripts_bucket    = module.s3.scripts_bucket_name
  pipeline_role_arn = module.iam.codepipeline_role_arn
  glue_job_name     = module.glue.job_name
}

# ─────────────────────────────────────────
# IAM – All roles
# ─────────────────────────────────────────
module "iam" {
  source      = "./modules/iam"
  project     = var.project
  environment = var.environment
  account_id  = data.aws_caller_identity.current.account_id
}

data "aws_caller_identity" "current" {}
