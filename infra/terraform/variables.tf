variable "region" {
  type    = string
  default = "ap-south-1" # Mumbai
}
variable "env" {
  type    = string
  default = "dev"
}
variable "snowflake_storage_aws_iam_user_arn" {
  description = "STORAGE_AWS_IAM_USER_ARN from DESC INTEGRATION s3_raw_int"
  type        = string
}
variable "snowflake_storage_external_id" {
  description = "STORAGE_AWS_EXTERNAL_ID from DESC INTEGRATION s3_raw_int"
  type        = string
}
variable "snowpipe_sqs_arn" {
  description = "notification_channel from SHOW PIPES (pipe_clickstream)"
  type        = string
}
variable "source_db_endpoint" {
  description = "shop_db PostgreSQL host (RDS)"
  type        = string
}
variable "vpc_id" { type = string }
variable "private_subnet_ids" { type = list(string) }
variable "alert_email" {
  type    = string
  default = "data-team@shopstream.example"
}
variable "dbt_image_uri" {
  description = "ECR image built by .github/workflows/deploy.yml"
  type        = string
}
