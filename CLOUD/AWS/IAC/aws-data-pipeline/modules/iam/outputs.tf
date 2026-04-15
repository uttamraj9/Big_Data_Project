output "glue_role_arn"         { value = aws_iam_role.glue.arn }
output "dms_role_arn"          { value = aws_iam_role.dms.arn }
output "redshift_role_arn"     { value = aws_iam_role.redshift.arn }
output "lambda_role_arn"       { value = aws_iam_role.lambda.arn }
output "codepipeline_role_arn" { value = aws_iam_role.codepipeline.arn }
