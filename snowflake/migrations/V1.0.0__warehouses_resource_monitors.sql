-- =====================================================================
-- V1.0.0  Virtual warehouses + cost guardrails
-- Run as: ACCOUNTADMIN (resource monitors) / SYSADMIN (warehouses)
-- Principle: one warehouse per workload -> isolation, right-sizing and
--            per-workload credit attribution.
-- =====================================================================
use role sysadmin;

-- Ingestion (COPY INTO / small MERGEs): tiny, suspends fast
create warehouse if not exists load_wh
  warehouse_size = 'XSMALL' auto_suspend = 60 auto_resume = true
  initially_suspended = true
  comment = 'COPY INTO, Snowpipe-adjacent MERGE tasks';

-- dbt transformations: bigger, short bursty runs
create warehouse if not exists transform_wh
  warehouse_size = 'SMALL' auto_suspend = 60 auto_resume = true
  initially_suspended = true
  statement_timeout_in_seconds = 3600
  comment = 'dbt build (scheduled)';

-- BI / analysts: multi-cluster for concurrency (Enterprise edition),
-- STANDARD scaling adds clusters as soon as queries queue.
create warehouse if not exists bi_wh
  warehouse_size = 'SMALL' auto_suspend = 300 auto_resume = true
  min_cluster_count = 1 max_cluster_count = 3 scaling_policy = 'STANDARD'
  initially_suspended = true
  statement_queued_timeout_in_seconds = 600
  comment = 'Power BI / Tableau / Streamlit';

-- Query Acceleration Service for spiky, scan-heavy ad hoc queries
alter warehouse bi_wh set enable_query_acceleration = true query_acceleration_max_scale_factor = 4;

-- ---------------------------------------------------------------------
-- Resource monitors: notify at 75/90%, suspend at 100%, hard stop at 110%
-- ---------------------------------------------------------------------
use role accountadmin;

create or replace resource monitor rm_account_monthly
  with credit_quota = 300 frequency = monthly start_timestamp = immediately
  triggers on 75 percent do notify
           on 90 percent do notify
           on 100 percent do suspend
           on 110 percent do suspend_immediate;
alter account set resource_monitor = rm_account_monthly;

create or replace resource monitor rm_transform_daily
  with credit_quota = 8 frequency = daily start_timestamp = immediately
  triggers on 80 percent do notify on 100 percent do suspend;
alter warehouse transform_wh set resource_monitor = rm_transform_daily;

create or replace resource monitor rm_bi_daily
  with credit_quota = 10 frequency = daily start_timestamp = immediately
  triggers on 80 percent do notify on 100 percent do suspend;
alter warehouse bi_wh set resource_monitor = rm_bi_daily;
