locals {
  prefix = "shopstream-${var.env}"
}

resource "aws_kms_key" "lake" {
  description             = "${local.prefix} data lake encryption"
  enable_key_rotation     = true
  deletion_window_in_days = 30
}

resource "aws_s3_bucket" "raw" {
  bucket = "shopstream-raw-${var.env}"
}

resource "aws_s3_bucket_versioning" "raw" {
  bucket = aws_s3_bucket.raw.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.lake.arn
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "raw" {
  bucket                  = aws_s3_bucket.raw.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Raw files are already in Snowflake after load: tier them down, expire quarantine
resource "aws_s3_bucket_lifecycle_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id
  rule {
    id     = "tier-landing"
    status = "Enabled"
    filter {}
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
    transition {
      days          = 180
      storage_class = "GLACIER_IR"
    }
    noncurrent_version_expiration { noncurrent_days = 30 }
  }
  rule {
    id     = "expire-quarantine"
    status = "Enabled"
    filter { prefix = "quarantine/" }
    expiration { days = 90 }
  }
}

# S3 events: raw clickstream -> validator Lambda; validated clickstream -> Snowpipe SQS
resource "aws_s3_bucket_notification" "raw" {
  bucket = aws_s3_bucket.raw.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.clickstream_validator.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "clickstream/"
    filter_suffix       = ".json.gz"
  }

  queue {
    queue_arn     = var.snowpipe_sqs_arn
    events        = ["s3:ObjectCreated:*"]
    filter_prefix = "clickstream_validated/"
  }

  depends_on = [aws_lambda_permission.allow_s3_invoke]
}
