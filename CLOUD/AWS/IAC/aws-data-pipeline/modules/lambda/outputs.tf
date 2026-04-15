output "function_arn"   { value = aws_lambda_function.schema_evaluator.arn }
output "function_name"  { value = aws_lambda_function.schema_evaluator.function_name }
output "sns_topic_arn"  { value = aws_sns_topic.schema_alerts.arn }
