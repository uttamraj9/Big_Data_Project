output "endpoint"           { value = aws_redshift_cluster.this.endpoint }
output "cluster_id"         { value = aws_redshift_cluster.this.cluster_identifier }
output "database"           { value = aws_redshift_cluster.this.database_name }
output "sns_topic_arn"      { value = aws_sns_topic.redshift_load.arn }
