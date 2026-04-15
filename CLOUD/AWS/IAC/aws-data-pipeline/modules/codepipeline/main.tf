# ─── CodeCommit Repository (stores Glue scripts) ─────────────────────────────
resource "aws_codecommit_repository" "glue_scripts" {
  repository_name = "${var.project}-glue-scripts-${var.environment}"
  description     = "Glue ETL scripts repository"
}

# ─── CodeBuild – validate/lint Glue script ────────────────────────────────────
resource "aws_iam_role" "codebuild" {
  name = "${var.project}-codebuild-role-${var.environment}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "codebuild.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "codebuild_policy" {
  name = "codebuild-policy"
  role = aws_iam_role.codebuild.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:GetBucketAcl", "s3:GetBucketLocation"]
        Resource = ["arn:aws:s3:::${var.scripts_bucket}", "arn:aws:s3:::${var.scripts_bucket}/*"]
      }
    ]
  })
}

resource "aws_codebuild_project" "validate_script" {
  name          = "${var.project}-validate-glue-${var.environment}"
  service_role  = aws_iam_role.codebuild.arn
  build_timeout = 10

  source {
    type = "CODEPIPELINE"
    buildspec = yamlencode({
      version = "0.2"
      phases = {
        install = {
          commands = ["pip install pyflakes"]
        }
        build = {
          commands = [
            "echo 'Linting Glue script...'",
            "pyflakes scripts/glue/bronze_to_silver.py || true",
            "echo 'Copying script to S3...'",
            "aws s3 cp scripts/glue/bronze_to_silver.py s3://${var.scripts_bucket}/glue/bronze_to_silver.py"
          ]
        }
      }
    })
  }

  artifacts {
    type = "CODEPIPELINE"
  }

  environment {
    compute_type = "BUILD_GENERAL1_SMALL"
    image        = "aws/codebuild/standard:7.0"
    type         = "LINUX_CONTAINER"
  }
}

# ─── CodePipeline ─────────────────────────────────────────────────────────────
resource "aws_codepipeline" "glue_pipeline" {
  name     = "${var.project}-pipeline-${var.environment}"
  role_arn = var.pipeline_role_arn

  artifact_store {
    location = var.scripts_bucket
    type     = "S3"
  }

  # Stage 1 – Source (CodeCommit)
  stage {
    name = "Source"
    action {
      name             = "Source"
      category         = "Source"
      owner            = "AWS"
      provider         = "CodeCommit"
      version          = "1"
      output_artifacts = ["source_output"]

      configuration = {
        RepositoryName       = aws_codecommit_repository.glue_scripts.repository_name
        BranchName           = "main"
        DetectChanges        = "true"
        OutputArtifactFormat = "CODE_ZIP"
      }
    }
  }

  # Stage 2 – Build / Lint
  stage {
    name = "Build"
    action {
      name             = "ValidateScript"
      category         = "Build"
      owner            = "AWS"
      provider         = "CodeBuild"
      version          = "1"
      input_artifacts  = ["source_output"]
      output_artifacts = ["build_output"]

      configuration = {
        ProjectName = aws_codebuild_project.validate_script.name
      }
    }
  }

  # Stage 3 – Deploy (trigger Glue job run)
  stage {
    name = "Deploy"
    action {
      name            = "TriggerGlueJob"
      category        = "Invoke"
      owner           = "AWS"
      provider        = "Glue"
      version         = "1"
      input_artifacts = ["build_output"]

      configuration = {
        JobName = var.glue_job_name
      }
    }
  }

  tags = { Project = var.project }
}
