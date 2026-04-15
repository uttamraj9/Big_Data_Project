# ─── Glue Database (Data Catalog) ────────────────────────────────────────────
resource "aws_glue_catalog_database" "bronze" {
  name = "${replace(var.project, "-", "_")}_bronze_${var.environment}"
}

resource "aws_glue_catalog_database" "silver" {
  name = "${replace(var.project, "-", "_")}_silver_${var.environment}"
}

# ─── Glue Crawler – Bronze ────────────────────────────────────────────────────
resource "aws_glue_crawler" "bronze" {
  name          = "${var.project}-bronze-crawler-${var.environment}"
  role          = var.glue_role_arn
  database_name = aws_glue_catalog_database.bronze.name

  s3_target {
    path = "s3://${var.bronze_bucket_name}/raw/"
  }

  configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Partitions = { AddOrUpdateBehavior = "InheritFromTable" }
    }
  })

  tags = { Project = var.project }
}

# ─── Glue Crawler – Silver ────────────────────────────────────────────────────
resource "aws_glue_crawler" "silver" {
  name          = "${var.project}-silver-crawler-${var.environment}"
  role          = var.glue_role_arn
  database_name = aws_glue_catalog_database.silver.name

  s3_target {
    path = "s3://${var.silver_bucket_name}/transformed/"
  }

  tags = { Project = var.project }
}

# ─── Glue ETL Job – Bronze to Silver ─────────────────────────────────────────
resource "aws_glue_job" "bronze_to_silver" {
  name         = "${var.project}-bronze-to-silver-${var.environment}"
  role_arn     = var.glue_role_arn
  glue_version = "4.0"

  command {
    name            = "glueetl"
    script_location = "s3://${var.scripts_bucket}/glue/bronze_to_silver.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"            = "s3://${var.scripts_bucket}/spark-logs/"
    "--SOURCE_BUCKET"                    = var.bronze_bucket_name
    "--TARGET_BUCKET"                    = var.silver_bucket_name
    "--SOURCE_PREFIX"                    = "raw/"
    "--TARGET_PREFIX"                    = "transformed/"
    "--TempDir"                          = "s3://${var.scripts_bucket}/tmp/"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  number_of_workers = 2
  worker_type       = "G.1X"

  tags = { Project = var.project }
}

# ─── Glue Trigger – run silver crawler after ETL job ─────────────────────────
resource "aws_glue_trigger" "post_etl" {
  name = "${var.project}-post-etl-trigger-${var.environment}"
  type = "CONDITIONAL"

  actions {
    crawler_name = aws_glue_crawler.silver.name
  }

  predicate {
    conditions {
      job_name = aws_glue_job.bronze_to_silver.name
      state    = "SUCCEEDED"
    }
  }

  tags = { Project = var.project }
}
