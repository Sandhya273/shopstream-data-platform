-- =====================================================================
-- V1.1.1  In-warehouse data quality objects (attached in post_dbt/)
--   * Data Metric Functions (DMFs) run on a schedule and write to
--     SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS
--   * Alerts email the team when freshness / reconciliation breaks
--   (pipeline-level DQ + reconciliation lives in data_quality/ python framework)
-- =====================================================================
use role transformer_role;

-- custom DMF: negative order totals
create or replace data metric function analytics_db.governance.negative_count(t table (amount number))
returns number as 'select count_if(amount < 0) from t';


-- ---- Email channel for alerts ----
use role accountadmin;
create notification integration if not exists email_data_team
  type = email enabled = true allowed_recipients = ('data-team@shopstream.example');

grant usage on integration email_data_team to role transformer_role;
