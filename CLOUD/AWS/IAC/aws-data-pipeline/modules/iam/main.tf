# ─── Glue Role ────────────────────────────────────────────────────────────────
resource "aws_iam_role" "glue" {
  name = "${var.project}-glue-role-${var.environment}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_s3" {
  name = "glue-s3-access"
  role = aws_iam_role.glue.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:*"]
      Resource = ["arn:aws:s3:::${var.project}-*", "arn:aws:s3:::${var.project}-*/*"]
    }]
  })
}

# ─── DMS Role ─────────────────────────────────────────────────────────────────
resource "aws_iam_role" "dms" {
  name = "${var.project}-dms-role-${var.environment}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "dms.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "dms_s3" {
  name = "dms-s3-access"
  role = aws_iam_role.dms.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:PutObject", "s3:GetObject", "s3:DeleteObject", "s3:ListBucket"]
      Resource = ["arn:aws:s3:::${var.project}-bronze-*", "arn:aws:s3:::${var.project}-bronze-*/*"]
    }]
  })
}

# DMS VPC role (required name by AWS)
resource "aws_iam_role" "dms_vpc" {
  name = "dms-vpc-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "dms.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "dms_vpc" {
  role       = aws_iam_role.dms_vpc.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonDMSVPCManagementRole"
}

# ─── Redshift Role ────────────────────────────────────────────────────────────
resource "aws_iam_role" "redshift" {
  name = "${var.project}-redshift-role-${var.environment}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "redshift.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "redshift_s3" {
  name = "redshift-s3-read"
  role = aws_iam_role.redshift.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:GetObject", "s3:ListBucket"]
      Resource = ["arn:aws:s3:::${var.project}-silver-*", "arn:aws:s3:::${var.project}-silver-*/*"]
    }]
  })
}

# ─── Lambda Role ──────────────────────────────────────────────────────────────
resource "aws_iam_role" "lambda" {
  name = "${var.project}-lambda-role-${var.environment}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_basic" {
  role       = aws_iam_role.lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "lambda_s3_glue" {
  name = "lambda-s3-glue"
  role = aws_iam_role.lambda.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:ListBucket", "s3:PutObject"]
        Resource = ["arn:aws:s3:::${var.project}-*", "arn:aws:s3:::${var.project}-*/*"]
      },
      {
        Effect   = "Allow"
        Action   = ["glue:StartJobRun", "glue:GetJob", "glue:GetJobRun"]
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = ["sns:Publish"]
        Resource = "*"
      }
    ]
  })
}

# ─── CodePipeline Role ────────────────────────────────────────────────────────
resource "aws_iam_role" "codepipeline" {
  name = "${var.project}-codepipeline-role-${var.environment}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "codepipeline.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "codepipeline_policy" {
  name = "codepipeline-policy"
  role = aws_iam_role.codepipeline.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:*"]
        Resource = ["arn:aws:s3:::${var.project}-*", "arn:aws:s3:::${var.project}-*/*"]
      },
      {
        Effect   = "Allow"
        Action   = ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJob", "glue:BatchStopJobRun"]
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = ["codebuild:BatchGetBuilds", "codebuild:StartBuild"]
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = ["codecommit:*"]
        Resource = "*"
      }
    ]
  })
}
