-- Operational queries for loading (run ad hoc, not part of deployments)
-- COPY INTO keeps 64 days of per-file load metadata, so re-running a COPY never
-- double-loads a file (idempotent) unless FORCE = TRUE.
use role loader_role;
use warehouse load_wh;
use schema raw_db.raw;

-- After creating the integration:
-- Copy STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID into the IAM role trust policy
describe integration s3_raw_int;


-- Stage checks
-- quick checks
list @stg_dms/orders/ pattern = '.*year=2026/month=10/.*';
select $1, $2, $3, metadata$filename from @stg_dms/orders/ (file_format => 'ff_dms_csv') limit 10;

-- ---- Pre-flight validation (no data loaded, returns parse errors) ----
copy into dms_orders from @stg_dms/orders/ validation_mode = 'RETURN_ERRORS';


-- Point the S3 bucket event notification at this SQS ARN (terraform: aws_s3_bucket_notification)
show pipes like 'pipe_clickstream';          -- column notification_channel
select system$pipe_status('pipe_clickstream');

-- Backfill files that landed before the pipe existed
alter pipe pipe_clickstream refresh prefix = 'dt=2026-10' modified_after = '2026-10-01T00:00:00Z';

-- ---- Load audit: what loaded, what failed (last 24h) ----
select file_name, table_name, row_count, row_parsed, error_count, first_error_message, status, last_load_time
from table(information_schema.copy_history(table_name => 'RAW_DB.RAW.CLICKSTREAM_EVENTS',
                                           start_time => dateadd(hour, -24, current_timestamp())))
order by last_load_time desc;

-- Task graph monitoring
-- Monitoring
select name, state, scheduled_time, completed_time, error_message
from table(information_schema.task_history(scheduled_time_range_start => dateadd(hour, -6, current_timestamp())))
order by scheduled_time desc;



-- =====================================================================
-- Batch COPY statements executed by the loader Lambda (hourly, Step Functions)
-- =====================================================================
-- DMS CDC: transform during load to capture file metadata
copy into dms_orders (op, cdc_ts, order_id, customer_id, order_ts, order_status, channel,
                      shipping_city, promo_code, shipping_fee, updated_at, _source_file, _file_row_number)
from (
  select $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, metadata$filename, metadata$file_row_number
  from @stg_dms/orders/
)
pattern = '.*[.]csv'
on_error = 'ABORT_STATEMENT';          -- CDC must be complete: never silently skip rows

-- Payments: Parquet -> VARIANT
copy into payments (raw, _source_file, _file_row_number)
from (
  select parse_json($1:raw::varchar), metadata$filename, metadata$file_row_number
  from @stg_payments
)
pattern = '.*part-.*[.]parquet'
on_error = 'ABORT_STATEMENT';

-- Re-load a corrected file that was already loaded once (explicit, audited)
-- copy into dms_orders from @stg_dms/orders/year=2026/month=10/orders_202610.csv force = true;
