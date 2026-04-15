# ─── Security Group ───────────────────────────────────────────────────────────
resource "aws_security_group" "redshift" {
  name        = "${var.project}-redshift-sg-${var.environment}"
  description = "Redshift cluster SG"
  vpc_id      = var.vpc_id

  ingress {
    from_port   = 5439
    to_port     = 5439
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# ─── Subnet Group ─────────────────────────────────────────────────────────────
resource "aws_redshift_subnet_group" "this" {
  name       = "${var.project}-redshift-subnet-${var.environment}"
  subnet_ids = var.subnet_ids
}

# ─── Redshift Cluster ─────────────────────────────────────────────────────────
resource "aws_redshift_cluster" "this" {
  cluster_identifier        = "${var.project}-cluster-${var.environment}"
  database_name             = "datawarehouse"
  master_username           = "admin"
  master_password           = var.db_password
  node_type                 = "dc2.large"
  cluster_type              = "single-node"
  number_of_nodes           = 1
  publicly_accessible       = true
  skip_final_snapshot       = true
  cluster_subnet_group_name = aws_redshift_subnet_group.this.name
  vpc_security_group_ids    = [aws_security_group.redshift.id]
  iam_roles                 = [var.redshift_role_arn]

  tags = { Project = var.project }
}

# ─── SNS for COPY job notifications ──────────────────────────────────────────
resource "aws_sns_topic" "redshift_load" {
  name = "${var.project}-redshift-load-notify-${var.environment}"
}

# ─── Redshift Scheduled Query (COPY from S3 Silver) ──────────────────────────
# The COPY command runs via a Lambda trigger (see lambda module).
# This resource stores the COPY SQL for reference / manual execution.
resource "aws_redshift_scheduled_action" "copy_from_silver" {
  name     = "${var.project}-copy-silver-${var.environment}"
  schedule = "cron(0 2 * * ? *)"   # daily at 02:00 UTC
  iam_role = var.redshift_role_arn

  target_action {
    resume_cluster {
      cluster_identifier = aws_redshift_cluster.this.cluster_identifier
    }
  }
}
