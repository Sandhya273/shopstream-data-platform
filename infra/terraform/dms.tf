# AWS DMS: full load + ongoing CDC from shop_db (PostgreSQL logical replication) to S3 as CSV
resource "aws_dms_replication_subnet_group" "this" {
  replication_subnet_group_id          = "${local.prefix}-dms"
  replication_subnet_group_description = "DMS subnets"
  subnet_ids                           = var.private_subnet_ids
}

resource "aws_dms_replication_instance" "this" {
  replication_instance_id     = "${local.prefix}-dms"
  replication_instance_class  = "dms.t3.medium"
  allocated_storage           = 50
  multi_az                    = false
  publicly_accessible         = false
  replication_subnet_group_id = aws_dms_replication_subnet_group.this.id
}

resource "aws_dms_endpoint" "source" {
  endpoint_id                     = "${local.prefix}-shopdb-source"
  endpoint_type                   = "source"
  engine_name                     = "postgres"
  server_name                     = var.source_db_endpoint
  port                            = 5432
  database_name                   = "shop_db"
  secrets_manager_arn             = aws_secretsmanager_secret.source_db.arn
  secrets_manager_access_role_arn = aws_iam_role.dms_secrets.arn
  ssl_mode                        = "require"
}

resource "aws_secretsmanager_secret" "source_db" { name = "${local.prefix}/shop-db/dms" }

resource "aws_iam_role" "dms_secrets" {
  name = "${local.prefix}-dms-secrets"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Principal = { Service = "dms.${var.region}.amazonaws.com" }, Action = "sts:AssumeRole" }]
  })
}

resource "aws_iam_role_policy" "dms_secrets" {
  role = aws_iam_role.dms_secrets.id
  policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Action = ["secretsmanager:GetSecretValue"], Resource = aws_secretsmanager_secret.source_db.arn }]
  })
}

resource "aws_iam_role" "dms_s3" {
  name = "${local.prefix}-dms-s3-target"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Principal = { Service = "dms.amazonaws.com" }, Action = "sts:AssumeRole" }]
  })
}

resource "aws_iam_role_policy" "dms_s3" {
  role = aws_iam_role.dms_s3.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = ["s3:PutObject", "s3:DeleteObject", "s3:PutObjectTagging"], Resource = "${aws_s3_bucket.raw.arn}/dms/*" },
      { Effect = "Allow", Action = ["s3:ListBucket"], Resource = aws_s3_bucket.raw.arn },
      { Effect = "Allow", Action = ["kms:GenerateDataKey", "kms:Decrypt"], Resource = aws_kms_key.lake.arn }
    ]
  })
}

resource "aws_dms_s3_endpoint" "target" {
  endpoint_id                       = "${local.prefix}-s3-target"
  endpoint_type                     = "target"
  bucket_name                       = aws_s3_bucket.raw.bucket
  bucket_folder                     = "dms"
  service_access_role_arn           = aws_iam_role.dms_s3.arn
  data_format                       = "csv"
  add_column_name                   = true            # header row
  include_op_for_full_load          = true            # Op column also on full load (I)
  timestamp_column_name             = "cdc_ts"
  date_partition_enabled            = true
  date_partition_sequence           = "YYYYMMDD"
  cdc_max_batch_interval            = 300             # flush every 5 min
  encryption_mode                   = "SSE_KMS"
  server_side_encryption_kms_key_id = aws_kms_key.lake.arn
}

resource "aws_dms_replication_task" "shopdb_cdc" {
  replication_task_id      = "${local.prefix}-shopdb-full-load-cdc"
  migration_type           = "full-load-and-cdc"
  replication_instance_arn = aws_dms_replication_instance.this.replication_instance_arn
  source_endpoint_arn      = aws_dms_endpoint.source.endpoint_arn
  target_endpoint_arn      = aws_dms_s3_endpoint.target.endpoint_arn
  table_mappings = jsonencode({
    rules = [for i, t in ["customers", "products", "orders", "order_items"] : {
      rule-type = "selection", rule-id = tostring(i + 1), rule-name = t, rule-action = "include"
      object-locator = { schema-name = "public", table-name = t }
    }]
  })
  replication_task_settings = jsonencode({
    Logging = { EnableLogging = true }
    ErrorBehavior = { DataErrorPolicy = "LOG_ERROR", TableErrorPolicy = "STOP_TASK" }
  })
  start_replication_task = true
}
