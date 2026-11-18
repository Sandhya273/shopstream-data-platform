# ---------------- Secrets (values set out-of-band, never in Terraform state) ----------------
resource "aws_secretsmanager_secret" "payments_api" { name = "${local.prefix}/payments-api" }
resource "aws_secretsmanager_secret" "snowflake_loader" { name = "${local.prefix}/snowflake/svc_loader" }
resource "aws_secretsmanager_secret" "snowflake_dbt" { name = "${local.prefix}/snowflake/svc_dbt" }

# ---------------- Lambda functions ----------------
data "archive_file" "ingestion" {
  type        = "zip"
  source_dir  = "${path.module}/build/lambda_src" # CI copies ./ingestion here so imports resolve as ingestion.*
  output_path = "${path.module}/build/ingestion.zip"
}

resource "aws_lambda_layer_version" "deps" {
  layer_name          = "${local.prefix}-ingestion-deps"
  filename            = "${path.module}/build/deps_layer.zip" # built in CI: pip install -r requirements-lambda.txt -t python/
  compatible_runtimes = ["python3.12"]
}

resource "aws_lambda_function" "clickstream_validator" {
  function_name    = "${local.prefix}-clickstream-validator"
  role             = aws_iam_role.lambda.arn
  runtime          = "python3.12"
  handler          = "ingestion/lambda_clickstream/handler.lambda_handler"
  filename         = data.archive_file.ingestion.output_path
  source_code_hash = data.archive_file.ingestion.output_base64sha256
  timeout          = 120
  memory_size      = 512
  layers           = [aws_lambda_layer_version.deps.arn]
  environment {
    variables = { STORAGE_BACKEND = "s3", RAW_BUCKET = aws_s3_bucket.raw.bucket }
  }
  dead_letter_config { target_arn = aws_sqs_queue.dlq.arn }
}

resource "aws_lambda_permission" "allow_s3_invoke" {
  statement_id  = "AllowS3Invoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.clickstream_validator.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.raw.arn
}

resource "aws_lambda_function" "payments_extractor" {
  function_name    = "${local.prefix}-payments-extractor"
  role             = aws_iam_role.lambda.arn
  runtime          = "python3.12"
  handler          = "ingestion/api_extractor/payments_extractor.lambda_handler"
  filename         = data.archive_file.ingestion.output_path
  source_code_hash = data.archive_file.ingestion.output_base64sha256
  timeout          = 900
  memory_size      = 1024
  layers           = [aws_lambda_layer_version.deps.arn]
  environment {
    variables = {
      STORAGE_BACKEND     = "s3"
      RAW_BUCKET          = aws_s3_bucket.raw.bucket
      PAYMENTS_API_URL    = "https://api.payments.example"
      PAYMENTS_API_SECRET = aws_secretsmanager_secret.payments_api.name
    }
  }
}

resource "aws_lambda_function" "snowflake_loader" {
  function_name    = "${local.prefix}-snowflake-loader"
  role             = aws_iam_role.lambda.arn
  runtime          = "python3.12"
  handler          = "ingestion/snowflake_loader/handler.lambda_handler"
  filename         = data.archive_file.ingestion.output_path
  source_code_hash = data.archive_file.ingestion.output_base64sha256
  timeout          = 900
  memory_size      = 512
  layers           = [aws_lambda_layer_version.deps.arn]
  environment {
    variables = { SNOWFLAKE_SECRET = aws_secretsmanager_secret.snowflake_loader.name }
  }
}

resource "aws_sqs_queue" "dlq" {
  name                      = "${local.prefix}-ingestion-dlq"
  message_retention_seconds = 1209600
}

# ---------------- Glue ----------------
resource "aws_s3_object" "glue_script" {
  bucket = aws_s3_bucket.raw.id
  key    = "glue/scripts/dms_cdc_to_parquet.py"
  source = "${path.module}/../../ingestion/glue_jobs/dms_cdc_to_parquet.py"
  etag   = filemd5("${path.module}/../../ingestion/glue_jobs/dms_cdc_to_parquet.py")
}

resource "aws_glue_job" "dms_compaction" {
  name              = "${local.prefix}-dms-cdc-to-parquet"
  role_arn          = aws_iam_role.glue.arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 60
  command {
    script_location = "s3://${aws_s3_bucket.raw.bucket}/${aws_s3_object.glue_script.key}"
    python_version  = "3"
  }
  default_arguments = {
    "--job-bookmark-option"              = "job-bookmark-enable"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
  }
}

# ---------------- ECS Fargate for dbt + DQ ----------------
resource "aws_ecs_cluster" "data" {
  name = "${local.prefix}-data"
  setting {
    name  = "containerInsights"
    value = "enabled"
  }
}

resource "aws_cloudwatch_log_group" "dbt" {
  name              = "/ecs/${local.prefix}-dbt"
  retention_in_days = 30
}

resource "aws_ecs_task_definition" "dbt" {
  family                   = "${local.prefix}-dbt"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 1024
  memory                   = 2048
  execution_role_arn       = aws_iam_role.ecs_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn
  container_definitions = jsonencode([{
    name      = "dbt"
    image     = var.dbt_image_uri
    essential = true
    command   = ["dbt", "build", "--target", "prod"]
    environment = [
      { name = "STORAGE_BACKEND", value = "s3" },
      { name = "RAW_BUCKET", value = aws_s3_bucket.raw.bucket }
    ]
    secrets = [
      { name = "SNOWFLAKE_ACCOUNT", valueFrom = "${aws_secretsmanager_secret.snowflake_dbt.arn}:account::" },
      { name = "SNOWFLAKE_PRIVATE_KEY", valueFrom = "${aws_secretsmanager_secret.snowflake_dbt.arn}:private_key::" }
    ]
    logConfiguration = {
      logDriver = "awslogs"
      options   = { awslogs-group = aws_cloudwatch_log_group.dbt.name, awslogs-region = var.region, awslogs-stream-prefix = "dbt" }
    }
  }])
}

resource "aws_security_group" "dbt" {
  name   = "${local.prefix}-dbt"
  vpc_id = var.vpc_id
  egress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"] # Snowflake + AWS APIs over HTTPS (use PrivateLink in prod)
  }
}
