output "raw_bucket" { value = aws_s3_bucket.raw.bucket }
output "snowflake_role_arn" {
  description = "Use as STORAGE_AWS_ROLE_ARN in snowflake/03_storage_integration_stages.sql"
  value       = aws_iam_role.snowflake_s3_access.arn
}
output "state_machine_arn" { value = aws_sfn_state_machine.pipeline.arn }
output "dbt_task_definition" { value = aws_ecs_task_definition.dbt.arn }
