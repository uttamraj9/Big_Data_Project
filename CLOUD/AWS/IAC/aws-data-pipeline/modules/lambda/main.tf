# ─── Package Lambda code ──────────────────────────────────────────────────────
data "archive_file" "schema_evaluator" {
  type        = "zip"
  source_file = "${path.module}/../../lambda/schema_evaluator.py"
  output_path = "${path.module}/../../lambda/schema_evaluator.zip"
}

# ─── Lambda Function ──────────────────────────────────────────────────────────
resource "aws_lambda_function" "schema_evaluator" {
  function_name    = "${var.project}-schema-evaluator-${var.environment}"
  role             = var.lambda_role_arn
  handler          = "schema_evaluator.lambda_handler"
  runtime          = "python3.12"
  timeout          = 300
  memory_size      = 512
  filename         = data.archive_file.schema_evaluator.output_path
  source_code_hash = data.archive_file.schema_evaluator.output_base64sha256

  environment {
    variables = {
      GLUE_JOB_NAME = "${var.project}-bronze-to-silver-${var.environment}"
      SCHEMA_BUCKET = var.bronze_bucket_name
      SCHEMA_KEY    = "schemas/expected_schema.json"
      TARGET_BUCKET = var.silver_bucket_name
      SNS_TOPIC_ARN = aws_sns_topic.schema_alerts.arn
    }
  }

  tags = { Project = var.project }
}

# ─── SNS Topic for schema alerts ──────────────────────────────────────────────
resource "aws_sns_topic" "schema_alerts" {
  name = "${var.project}-schema-alerts-${var.environment}"
}

# ─── Allow S3 to invoke Lambda ────────────────────────────────────────────────
resource "aws_lambda_permission" "allow_s3_bronze" {
  statement_id  = "AllowS3BronzeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.schema_evaluator.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = "arn:aws:s3:::${var.bronze_bucket_name}"
}

# ─── S3 event notification → Lambda ──────────────────────────────────────────
resource "aws_s3_bucket_notification" "bronze_trigger" {
  bucket = var.bronze_bucket_name

  lambda_function {
    lambda_function_arn = aws_lambda_function.schema_evaluator.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "raw/"
    filter_suffix       = ".parquet"
  }

  depends_on = [aws_lambda_permission.allow_s3_bronze]
}
