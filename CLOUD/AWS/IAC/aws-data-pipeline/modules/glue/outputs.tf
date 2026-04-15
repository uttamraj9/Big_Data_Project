output "job_name"           { value = aws_glue_job.bronze_to_silver.name }
output "bronze_database"    { value = aws_glue_catalog_database.bronze.name }
output "silver_database"    { value = aws_glue_catalog_database.silver.name }
output "bronze_crawler"     { value = aws_glue_crawler.bronze.name }
output "silver_crawler"     { value = aws_glue_crawler.silver.name }
