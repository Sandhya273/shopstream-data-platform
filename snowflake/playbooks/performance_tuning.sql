-- =====================================================================
-- 08  Performance tuning & troubleshooting playbook
-- =====================================================================
use role transformer_role;
use warehouse transform_wh;

-- ---------------------------------------------------------------------
-- A. Micro-partition pruning & clustering
--    Large facts are filtered by date -> cluster on date so each
--    micro-partition holds a narrow date range and filters prune.
-- ---------------------------------------------------------------------
alter table analytics_db.marts.fct_orders cluster by (order_date);   -- (also set via dbt cluster_by)

-- How well clustered? average_depth close to 1 = good, many overlaps = poor pruning
select system$clustering_information('analytics_db.marts.fct_orders', '(order_date)');
select system$clustering_depth('analytics_db.marts.fct_orders', '(order_date)');

-- Proof of pruning: compare partitions scanned vs total for a date filter
select count(*), sum(recognised_revenue) from analytics_db.marts.fct_orders
where order_date between '2026-10-01' and '2026-10-31';
select query_id, partitions_scanned, partitions_total, bytes_scanned
from table(information_schema.query_history_by_session()) order by start_time desc limit 1;

-- Anti-pattern that defeats pruning (function on the filtered column):
--   where to_char(order_date, 'YYYY-MM') = '2026-10'        -- may scan more partitions
-- Prefer a range predicate on the raw column:
--   where order_date >= '2026-10-01' and order_date < '2026-11-01'

-- Automatic clustering credits (keep an eye on cost vs benefit)
select table_name, sum(credits_used) credits, sum(num_rows_reclustered) rows_reclustered
from snowflake.account_usage.automatic_clustering_history
where start_time > dateadd(day, -30, current_timestamp()) group by 1 order by 2 desc;

-- ---------------------------------------------------------------------
-- B. Search Optimization for selective point lookups (support team looks up payment_id)
-- ---------------------------------------------------------------------
alter table analytics_db.marts.fct_payments add search optimization on equality(payment_id, order_id);
select * from analytics_db.marts.fct_payments where payment_id = 'pay_0028ae9963b39bafacdd';

-- ---------------------------------------------------------------------
-- C. Find the expensive / slow queries (last 7 days)
-- ---------------------------------------------------------------------
select query_id, user_name, warehouse_name, warehouse_size,
       total_elapsed_time / 1000                        as elapsed_s,
       queued_overload_time / 1000                      as queued_s,      -- warehouse too small / concurrency
       bytes_spilled_to_local_storage / power(1024, 3)  as spill_local_gb, -- memory pressure
       bytes_spilled_to_remote_storage / power(1024, 3) as spill_remote_gb,-- severe: size up or fix the join
       partitions_scanned, partitions_total,
       round(partitions_scanned / nullif(partitions_total, 0), 2) as scan_ratio,  -- ~1.0 = no pruning
       left(query_text, 120) as query_snippet
from snowflake.account_usage.query_history
where start_time > dateadd(day, -7, current_timestamp())
  and execution_status = 'SUCCESS'
  and warehouse_name is not null
order by total_elapsed_time desc
limit 25;

-- Operator-level profile of one query: find exploding joins, spills, full scans
select operator_id, operator_type, operator_statistics:output_rows::number as out_rows,
       execution_time_breakdown:overall_percentage::float as pct_time,
       operator_statistics:spilling as spilling, operator_attributes
from table(get_query_operator_stats('<query_id>'))
order by pct_time desc;

-- Repeated identical queries -> candidates for result cache / materialization
select query_parameterized_hash, count(*) runs, avg(total_elapsed_time)/1000 avg_s, any_value(left(query_text, 100))
from snowflake.account_usage.query_history
where start_time > dateadd(day, -7, current_timestamp())
group by 1 having count(*) > 50 order by runs desc;

-- ---------------------------------------------------------------------
-- D. Warehouse sizing: load vs queueing
-- ---------------------------------------------------------------------
select date_trunc('hour', start_time) hr, warehouse_name,
       avg(avg_running) running, avg(avg_queued_load) queued
from snowflake.account_usage.warehouse_load_history
where start_time > dateadd(day, -7, current_timestamp())
group by 1, 2 order by 1 desc;
-- queued > 0 for BI_WH at peaks  -> raise max_cluster_count (concurrency)
-- long single queries + spilling -> raise warehouse_size (scale up)

-- ---------------------------------------------------------------------
-- E. Credit consumption & cost attribution
-- ---------------------------------------------------------------------
select warehouse_name, date_trunc('day', start_time) day,
       sum(credits_used) credits, sum(credits_used_cloud_services) cloud_services
from snowflake.account_usage.warehouse_metering_history
where start_time > dateadd(day, -30, current_timestamp())
group by 1, 2 order by 2 desc, 3 desc;

-- Cost per dbt model (query_tag set in profiles.yml; dbt adds node id in comments)
select query_tag, count(*) queries, sum(total_elapsed_time)/1000/3600 hours,
       sum(credits_used_cloud_services) cloud_credits
from snowflake.account_usage.query_history
where query_tag like 'dbt_%' and start_time > dateadd(day, -7, current_timestamp())
group by 1 order by hours desc;

-- Serverless features (Snowpipe, tasks, clustering, search optimization)
select service_type, sum(credits_used) credits
from snowflake.account_usage.metering_history
where start_time > dateadd(day, -30, current_timestamp())
group by 1 order by 2 desc;

-- Storage: tables with large Time Travel / Fail-safe footprint
select table_catalog, table_schema, table_name,
       active_bytes/power(1024,3) active_gb, time_travel_bytes/power(1024,3) tt_gb, failsafe_bytes/power(1024,3) fs_gb
from snowflake.account_usage.table_storage_metrics
where deleted = false order by fs_gb + tt_gb desc limit 20;

-- ---------------------------------------------------------------------
-- F. Quick wins checklist (applied in this project)
--   * auto_suspend 60s on batch warehouses; per-workload warehouses
--   * incremental MERGE models with lookback instead of full rebuilds
--   * transient staging/intermediate schemas (no fail-safe)
--   * cluster big facts on the most common filter (date)
--   * select only needed columns from VARIANT; avoid SELECT * in marts
--   * QUALIFY instead of self-joins for dedup; avoid DISTINCT to hide join fan-out
-- ---------------------------------------------------------------------
