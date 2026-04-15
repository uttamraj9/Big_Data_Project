# ─── Athena Workgroup ─────────────────────────────────────────────────────────
resource "aws_athena_workgroup" "this" {
  name          = "${var.project}-workgroup-${var.environment}"
  force_destroy = true

  configuration {
    result_configuration {
      output_location = "s3://${var.results_bucket}/athena-results/"
    }
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
  }

  tags = { Project = var.project }
}

# ─── Athena Database ──────────────────────────────────────────────────────────
resource "aws_athena_database" "silver" {
  name   = "${replace(var.project, "-", "_")}_silver_athena_${var.environment}"
  bucket = var.results_bucket
  force_destroy = true
}

# ─── Saved Queries ────────────────────────────────────────────────────────────
resource "aws_athena_named_query" "sample_select" {
  name        = "sample-select-silver"
  workgroup   = aws_athena_workgroup.this.id
  database    = aws_athena_database.silver.name
  description = "Sample SELECT from Silver layer"
  query       = <<-SQL
    SELECT *
    FROM "${aws_athena_database.silver.name}"."silver_data"
    LIMIT 100;
  SQL
}

resource "aws_athena_named_query" "count_rows" {
  name        = "count-silver-rows"
  workgroup   = aws_athena_workgroup.this.id
  database    = aws_athena_database.silver.name
  description = "Count rows in Silver layer"
  query       = <<-SQL
    SELECT COUNT(*) AS total_rows
    FROM "${aws_athena_database.silver.name}"."silver_data";
  SQL
}
