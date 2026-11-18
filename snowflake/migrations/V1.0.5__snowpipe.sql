-- =====================================================================
-- V1.0.5  Continuous loading: Snowpipe auto-ingest for clickstream
--   Batch COPY INTO for DMS + payments is executed by the loader Lambda
--   (ingestion/snowflake_loader/handler.py); the statements it runs are
--   documented in playbooks/loading_operations.sql.
-- =====================================================================
use role accountadmin;

-- SNS topic receives Snowpipe file-level load errors
create notification integration if not exists notify_pipe_errors
  enabled = true
  type = queue
  notification_provider = aws_sns
  direction = outbound
  aws_sns_topic_arn = 'arn:aws:sns:ap-south-1:<AWS_ACCOUNT_ID>:shopstream-<env>-pipeline-alerts'
  aws_sns_role_arn = 'arn:aws:iam::<AWS_ACCOUNT_ID>:role/shopstream-snowflake-sns-publish';
grant usage on integration notify_pipe_errors to role loader_role;

use role loader_role;
use schema raw_db.raw;

create pipe if not exists pipe_clickstream
  auto_ingest = true
  error_integration = notify_pipe_errors
  comment = 'Validated clickstream from the Lambda validator'
as
copy into clickstream_events (raw, _source_file, _file_row_number)
from (select $1, metadata$filename, metadata$file_row_number from @stg_clickstream)
on_error = 'SKIP_FILE_1%';           -- tolerate tiny corruption; failed files appear in COPY_HISTORY
