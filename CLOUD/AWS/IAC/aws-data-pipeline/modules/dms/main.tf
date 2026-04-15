# ─── Security Group for DMS ───────────────────────────────────────────────────
resource "aws_security_group" "dms" {
  name        = "${var.project}-dms-sg-${var.environment}"
  description = "DMS replication instance SG"
  vpc_id      = var.vpc_id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# ─── DMS Subnet Group ─────────────────────────────────────────────────────────
resource "aws_dms_replication_subnet_group" "this" {
  replication_subnet_group_id          = "${var.project}-dms-subnet-group"
  replication_subnet_group_description = "DMS subnet group for ${var.project}"
  subnet_ids                           = var.subnet_ids
}

# ─── DMS Replication Instance ─────────────────────────────────────────────────
resource "aws_dms_replication_instance" "this" {
  replication_instance_id     = "${var.project}-dms-${var.environment}"
  replication_instance_class  = "dms.t3.micro"
  allocated_storage           = 20
  publicly_accessible         = true
  multi_az                    = false
  vpc_security_group_ids      = [aws_security_group.dms.id]
  replication_subnet_group_id = aws_dms_replication_subnet_group.this.id

  tags = { Project = var.project }
}

# ─── Source Endpoint – PostgreSQL ─────────────────────────────────────────────
resource "aws_dms_endpoint" "source" {
  endpoint_id   = "${var.project}-source-postgres"
  endpoint_type = "source"
  engine_name   = "postgres"

  server_name   = var.source_db_host
  port          = var.source_db_port
  database_name = var.source_db_name
  username      = var.source_db_user
  password      = var.source_db_password

  tags = { Project = var.project }
}

# ─── Target Endpoint – S3 Bronze ──────────────────────────────────────────────
resource "aws_dms_endpoint" "target_s3" {
  endpoint_id   = "${var.project}-target-s3-bronze"
  endpoint_type = "target"
  engine_name   = "s3"

  s3_settings {
    bucket_name             = var.bronze_bucket_name
    bucket_folder           = "raw"
    service_access_role_arn = var.dms_role_arn
    data_format             = "parquet"
    compression_type        = "NONE"
    include_op_for_full_load = true
  }

  tags = { Project = var.project }
}

# ─── DMS Replication Task ─────────────────────────────────────────────────────
resource "aws_dms_replication_task" "this" {
  replication_task_id      = "${var.project}-dms-task-${var.environment}"
  migration_type           = "full-load-and-cdc"
  replication_instance_arn = aws_dms_replication_instance.this.replication_instance_arn
  source_endpoint_arn      = aws_dms_endpoint.source.endpoint_arn
  target_endpoint_arn      = aws_dms_endpoint.target_s3.endpoint_arn

  table_mappings = jsonencode({
    rules = [{
      rule-type = "selection"
      rule-id   = "1"
      rule-name = "include-all"
      object-locator = {
        schema-name = "%"
        table-name  = "%"
      }
      rule-action = "include"
    }]
  })

  replication_task_settings = jsonencode({
    TargetMetadata = {
      TargetSchema              = ""
      SupportLobs               = true
      FullLobMode               = false
      LobChunkSize              = 64
      LimitedSizeLobMode        = true
      LobMaxSize                = 32
    }
    FullLoadSettings = {
      TargetTablePrepMode       = "DROP_AND_CREATE"
      CreatePkAfterFullLoad     = false
      StopTaskCachedChangesApplied = false
      StopTaskCachedChangesNotApplied = false
      MaxFullLoadSubTasks       = 8
      TransactionConsistencyTimeout = 600
      CommitRate                = 50000
    }
    Logging = {
      EnableLogging = true
    }
  })

  tags = { Project = var.project }
}
