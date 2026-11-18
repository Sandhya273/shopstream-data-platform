-- Governance audit queries (needs IMPORTED PRIVILEGES on the SNOWFLAKE database)
use role accountadmin;

-- ---- Who accessed PII? (ACCOUNT_USAGE, ~3h latency) ----
select ah.query_start_time, ah.user_name, obj.value:objectName::string as object_name, col.value:columnName::string as column_name
from snowflake.account_usage.access_history ah,
     lateral flatten(ah.base_objects_accessed) obj,
     lateral flatten(obj.value:columns) col
where obj.value:objectName::string ilike 'ANALYTICS_DB.MARTS.DIM_CUSTOMER'
  and col.value:columnName::string in ('EMAIL', 'PHONE')
  and ah.query_start_time > dateadd(day, -7, current_timestamp())
order by 1 desc;

-- ---- Where is tagged data? ----
select * from snowflake.account_usage.tag_references where tag_name = 'PII_TYPE';


-- Latest data metric function results
select measurement_time, table_name, metric_name, value
from snowflake.local.data_quality_monitoring_results
order by measurement_time desc limit 50;

