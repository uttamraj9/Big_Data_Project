output "database_name"  { value = aws_athena_database.silver.name }
output "workgroup_name" { value = aws_athena_workgroup.this.name }
