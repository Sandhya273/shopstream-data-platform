resource "aws_sns_topic" "alerts" { name = "${local.prefix}-pipeline-alerts" }

resource "aws_sns_topic_subscription" "email" {
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

resource "aws_cloudwatch_log_group" "sfn" {
  name              = "/aws/vendedlogs/states/${local.prefix}-pipeline"
  retention_in_days = 30
}

resource "aws_sfn_state_machine" "pipeline" {
  name     = "${local.prefix}-batch-pipeline"
  role_arn = aws_iam_role.sfn.arn
  definition = templatefile("${path.module}/../../orchestration/step_functions/shopstream_pipeline.asl.json", {
    payments_extractor_arn  = aws_lambda_function.payments_extractor.arn
    snowflake_loader_arn    = aws_lambda_function.snowflake_loader.arn
    glue_job_name           = aws_glue_job.dms_compaction.name
    raw_bucket              = aws_s3_bucket.raw.bucket
    ecs_cluster_arn         = aws_ecs_cluster.data.arn
    dbt_task_definition_arn = aws_ecs_task_definition.dbt.arn
    private_subnets         = jsonencode(var.private_subnet_ids)
    dbt_sg                  = aws_security_group.dbt.id
    sns_topic_arn           = aws_sns_topic.alerts.arn
  })
  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.sfn.arn}:*"
    include_execution_data = true
    level                  = "ERROR"
  }
}

# ---------------- EventBridge Scheduler: hourly batch run ----------------
resource "aws_iam_role" "scheduler" {
  name = "${local.prefix}-scheduler"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Principal = { Service = "scheduler.amazonaws.com" }, Action = "sts:AssumeRole" }]
  })
}

resource "aws_iam_role_policy" "scheduler" {
  role = aws_iam_role.scheduler.id
  policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Action = ["states:StartExecution"], Resource = aws_sfn_state_machine.pipeline.arn }]
  })
}

resource "aws_scheduler_schedule" "hourly" {
  name                         = "${local.prefix}-hourly-pipeline"
  schedule_expression          = "cron(5 * * * ? *)"
  schedule_expression_timezone = "Asia/Kolkata"
  flexible_time_window { mode = "OFF" }
  target {
    arn      = aws_sfn_state_machine.pipeline.arn
    role_arn = aws_iam_role.scheduler.arn
    input = jsonencode({
      full_refresh = false
      dms_tables   = ["customers", "products", "orders", "order_items"]
    })
    retry_policy { maximum_retry_attempts = 2 }
  }
}

# ---------------- EventBridge rule: alert on failed / timed-out executions ----------------
resource "aws_cloudwatch_event_rule" "sfn_failed" {
  name = "${local.prefix}-pipeline-failed"
  event_pattern = jsonencode({
    source      = ["aws.states"]
    detail-type = ["Step Functions Execution Status Change"]
    detail = {
      status          = ["FAILED", "TIMED_OUT", "ABORTED"]
      stateMachineArn = [aws_sfn_state_machine.pipeline.arn]
    }
  })
}

resource "aws_cloudwatch_event_target" "sfn_failed_sns" {
  rule = aws_cloudwatch_event_rule.sfn_failed.name
  arn  = aws_sns_topic.alerts.arn
}

resource "aws_sns_topic_policy" "alerts" {
  arn = aws_sns_topic.alerts.arn
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow", Principal = { Service = "events.amazonaws.com" }, Action = "sns:Publish", Resource = aws_sns_topic.alerts.arn
    }]
  })
}

# Lambda error alarm (validator)
resource "aws_cloudwatch_metric_alarm" "validator_errors" {
  alarm_name          = "${local.prefix}-clickstream-validator-errors"
  namespace           = "AWS/Lambda"
  metric_name         = "Errors"
  dimensions          = { FunctionName = aws_lambda_function.clickstream_validator.function_name }
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  alarm_actions       = [aws_sns_topic.alerts.arn]
}
