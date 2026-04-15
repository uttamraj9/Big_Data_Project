output "bronze_bucket" {
  value = module.s3.bronze_bucket_name
}

output "silver_bucket" {
  value = module.s3.silver_bucket_name
}

output "redshift_endpoint" {
  value = module.redshift.endpoint
}

output "glue_job_name" {
  value = module.glue.job_name
}

output "athena_database" {
  value = module.athena.database_name
}

output "dms_task_arn" {
  value = module.dms.task_arn
}
