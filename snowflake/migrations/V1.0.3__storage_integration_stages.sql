-- =====================================================================
-- V1.0.3  S3 storage integration, file formats, external stages
--   Snowflake assumes an IAM role (infra/terraform: snowflake_s3_access)
--   -> no AWS keys stored in Snowflake.
-- =====================================================================
use role accountadmin;

create storage integration if not exists s3_raw_int
  type = external_stage
  storage_provider = 'S3'
  enabled = true
  storage_aws_role_arn = 'arn:aws:iam::<AWS_ACCOUNT_ID>:role/shopstream-snowflake-s3-access'
  storage_allowed_locations = ('s3://shopstream-raw-<env>/');

grant usage on integration s3_raw_int to role loader_role;

use role loader_role;
use schema raw_db.raw;

create file format if not exists ff_dms_csv
  type = csv field_delimiter = ',' skip_header = 1
  field_optionally_enclosed_by = '"' null_if = ('', 'NULL')
  empty_field_as_null = true trim_space = true
  error_on_column_count_mismatch = true;

create file format if not exists ff_json
  type = json strip_outer_array = false compression = auto
  ignore_utf8_errors = false;

create file format if not exists ff_parquet type = parquet compression = snappy;

create stage if not exists stg_dms
  storage_integration = s3_raw_int url = 's3://shopstream-raw-<env>/dms/' file_format = ff_dms_csv;

create stage if not exists stg_payments
  storage_integration = s3_raw_int url = 's3://shopstream-raw-<env>/raw/payments/' file_format = ff_parquet;

create stage if not exists stg_clickstream
  storage_integration = s3_raw_int url = 's3://shopstream-raw-<env>/clickstream_validated/' file_format = ff_json;

