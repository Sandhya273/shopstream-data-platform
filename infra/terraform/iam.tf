data "aws_caller_identity" "me" {}

# ---------------- Snowflake storage integration role (no keys in Snowflake) ----------------
resource "aws_iam_role" "snowflake_s3_access" {
  name = "shopstream-snowflake-s3-access"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { AWS = var.snowflake_storage_aws_iam_user_arn }
      Action    = "sts:AssumeRole"
      Condition = { StringEquals = { "sts:ExternalId" = var.snowflake_storage_external_id } }
    }]
  })
}

resource "aws_iam_role_policy" "snowflake_s3_read" {
  role = aws_iam_role.snowflake_s3_access.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = ["s3:GetObject", "s3:GetObjectVersion"], Resource = "${aws_s3_bucket.raw.arn}/*" },
      { Effect = "Allow", Action = ["s3:ListBucket", "s3:GetBucketLocation"], Resource = aws_s3_bucket.raw.arn },
      { Effect = "Allow", Action = ["kms:Decrypt"], Resource = aws_kms_key.lake.arn }
    ]
  })
}

# ---------------- Lambda execution role (least privilege per prefix) ----------------
data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "lambda" {
  name               = "${local.prefix}-ingestion-lambda"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

resource "aws_iam_role_policy_attachment" "lambda_logs" {
  role       = aws_iam_role.lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}

resource "aws_iam_role_policy" "lambda_data" {
  role = aws_iam_role.lambda.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = ["s3:GetObject"], Resource = ["${aws_s3_bucket.raw.arn}/clickstream/*", "${aws_s3_bucket.raw.arn}/_state/*"] },
      { Effect = "Allow", Action = ["s3:PutObject"], Resource = [
        "${aws_s3_bucket.raw.arn}/clickstream_validated/*", "${aws_s3_bucket.raw.arn}/quarantine/*",
        "${aws_s3_bucket.raw.arn}/_audit/*", "${aws_s3_bucket.raw.arn}/raw/payments/*", "${aws_s3_bucket.raw.arn}/_state/*"] },
      { Effect = "Allow", Action = ["s3:ListBucket"], Resource = aws_s3_bucket.raw.arn },
      { Effect = "Allow", Action = ["kms:Decrypt", "kms:GenerateDataKey"], Resource = aws_kms_key.lake.arn },
      { Effect = "Allow", Action = ["secretsmanager:GetSecretValue"], Resource = [aws_secretsmanager_secret.payments_api.arn, aws_secretsmanager_secret.snowflake_loader.arn] }
    ]
  })
}

# ---------------- Glue role ----------------
resource "aws_iam_role" "glue" {
  name = "${local.prefix}-glue"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Principal = { Service = "glue.amazonaws.com" }, Action = "sts:AssumeRole" }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_s3" {
  role = aws_iam_role.glue.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"], Resource = ["${aws_s3_bucket.raw.arn}/dms/*", "${aws_s3_bucket.raw.arn}/dms_parquet/*", "${aws_s3_bucket.raw.arn}/glue/*"] },
      { Effect = "Allow", Action = ["s3:ListBucket"], Resource = aws_s3_bucket.raw.arn },
      { Effect = "Allow", Action = ["kms:Decrypt", "kms:GenerateDataKey"], Resource = aws_kms_key.lake.arn }
    ]
  })
}

# ---------------- Step Functions role ----------------
resource "aws_iam_role" "sfn" {
  name = "${local.prefix}-pipeline-sfn"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Principal = { Service = "states.amazonaws.com" }, Action = "sts:AssumeRole" }]
  })
}

resource "aws_iam_role_policy" "sfn" {
  role = aws_iam_role.sfn.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = ["lambda:InvokeFunction"], Resource = [aws_lambda_function.payments_extractor.arn, aws_lambda_function.snowflake_loader.arn] },
      { Effect = "Allow", Action = ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns", "glue:BatchStopJobRun"], Resource = aws_glue_job.dms_compaction.arn },
      { Effect = "Allow", Action = ["ecs:RunTask", "ecs:StopTask", "ecs:DescribeTasks"], Resource = "*" },
      { Effect = "Allow", Action = ["iam:PassRole"], Resource = [aws_iam_role.ecs_task.arn, aws_iam_role.ecs_execution.arn] },
      { Effect = "Allow", Action = ["events:PutTargets", "events:PutRule", "events:DescribeRule"], Resource = "*" },
      { Effect = "Allow", Action = ["sns:Publish"], Resource = aws_sns_topic.alerts.arn }
    ]
  })
}

# ---------------- ECS (dbt / DQ container) ----------------
resource "aws_iam_role" "ecs_execution" {
  name = "${local.prefix}-ecs-execution"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Principal = { Service = "ecs-tasks.amazonaws.com" }, Action = "sts:AssumeRole" }]
  })
}

resource "aws_iam_role_policy_attachment" "ecs_execution" {
  role       = aws_iam_role.ecs_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role_policy" "ecs_execution_secrets" {
  role = aws_iam_role.ecs_execution.id
  policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Action = ["secretsmanager:GetSecretValue"], Resource = aws_secretsmanager_secret.snowflake_dbt.arn }]
  })
}

resource "aws_iam_role" "ecs_task" {
  name = "${local.prefix}-ecs-task"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Principal = { Service = "ecs-tasks.amazonaws.com" }, Action = "sts:AssumeRole" }]
  })
}

resource "aws_iam_role_policy" "ecs_task_s3_read" {
  role = aws_iam_role.ecs_task.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = ["s3:GetObject", "s3:ListBucket"], Resource = [aws_s3_bucket.raw.arn, "${aws_s3_bucket.raw.arn}/*"] },
      { Effect = "Allow", Action = ["kms:Decrypt"], Resource = aws_kms_key.lake.arn }
    ]
  })
}
