resource "aws_s3_bucket" "bronze" {
  bucket        = "${var.project}-bronze-${var.environment}"
  force_destroy = true
  tags = { Layer = "bronze", Project = var.project }
}

resource "aws_s3_bucket" "silver" {
  bucket        = "${var.project}-silver-${var.environment}"
  force_destroy = true
  tags = { Layer = "silver", Project = var.project }
}

resource "aws_s3_bucket" "scripts" {
  bucket        = "${var.project}-glue-scripts-${var.environment}"
  force_destroy = true
  tags = { Layer = "scripts", Project = var.project }
}

resource "aws_s3_bucket" "athena_results" {
  bucket        = "${var.project}-athena-results-${var.environment}"
  force_destroy = true
  tags = { Layer = "athena", Project = var.project }
}

# Block all public access on all buckets
resource "aws_s3_bucket_public_access_block" "bronze" {
  bucket                  = aws_s3_bucket.bronze.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_public_access_block" "silver" {
  bucket                  = aws_s3_bucket.silver.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Enable versioning on silver (production data)
resource "aws_s3_bucket_versioning" "silver" {
  bucket = aws_s3_bucket.silver.id
  versioning_configuration { status = "Enabled" }
}

# Upload Glue ETL script to scripts bucket
resource "aws_s3_object" "glue_script" {
  bucket = aws_s3_bucket.scripts.id
  key    = "glue/bronze_to_silver.py"
  source = "${path.module}/../../scripts/glue/bronze_to_silver.py"
  etag   = filemd5("${path.module}/../../scripts/glue/bronze_to_silver.py")
}
