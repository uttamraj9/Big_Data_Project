output "bronze_bucket_name" { value = aws_s3_bucket.bronze.bucket }
output "silver_bucket_name" { value = aws_s3_bucket.silver.bucket }
output "scripts_bucket_name" { value = aws_s3_bucket.scripts.bucket }
output "athena_results_bucket_name" { value = aws_s3_bucket.athena_results.bucket }
output "bronze_bucket_arn" { value = aws_s3_bucket.bronze.arn }
output "silver_bucket_arn" { value = aws_s3_bucket.silver.arn }
