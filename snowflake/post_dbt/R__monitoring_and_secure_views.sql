-- =====================================================================
-- Repeatable script (schemachange R__), run by the deploy workflow AFTER the
-- first dbt build so the tables exist. Re-runs only when this file changes;
-- after a dbt --full-refresh of fct_orders, re-run it to re-attach the DMFs.
-- =====================================================================
use role transformer_role;

-- ---- Data Metric Functions on gold tables (fct_orders is incremental -> not re-created) ----
alter table analytics_db.marts.fct_orders set data_metric_schedule = 'TRIGGER_ON_CHANGES';
alter table analytics_db.marts.fct_orders add data metric function snowflake.core.null_count on (customer_sk);
alter table analytics_db.marts.fct_orders add data metric function snowflake.core.duplicate_count on (order_id);
alter table analytics_db.marts.fct_orders add data metric function snowflake.core.freshness on (_dbt_loaded_at);

-- (dim_customer is re-created by dbt each run, so its checks live in dbt tests instead)

alter table analytics_db.marts.fct_orders add data metric function analytics_db.governance.negative_count on (order_total);

-- ---- Alerts ----
create or replace alert analytics_db.governance.alert_orders_stale
  warehouse = load_wh
  schedule = '60 MINUTE'
  if (exists (
      select 1 from analytics_db.marts.fct_orders
      having max(_dbt_loaded_at) < dateadd(hour, -3, current_timestamp())))
  then call system$send_email('email_data_team', 'data-team@shopstream.example',
       '[ShopStream] fct_orders is stale', 'fct_orders has not been refreshed for 3+ hours. Check the Step Functions run.');

create or replace alert analytics_db.governance.alert_payment_exceptions
  warehouse = load_wh
  schedule = 'USING CRON 0 8 * * * Asia/Kolkata'
  if (exists (
      select 1 from analytics_db.marts.mart_payment_reconciliation
      where recon_status in ('amount_mismatch', 'missing_payment')
        and order_date = current_date() - 1))
  then call system$send_email('email_data_team', 'finance-ops@shopstream.example',
       '[ShopStream] Payment reconciliation exceptions', 'Yesterday has orders that do not reconcile. See Power BI > Reconciliation.');

alter alert analytics_db.governance.alert_orders_stale resume;
alter alert analytics_db.governance.alert_payment_exceptions resume;

-- ---- Secure view for an external partner (hides definition, no optimizer leakage) ----
create or replace secure view analytics_db.marts.v_partner_daily_sales as
select order_date, channel, sum(orders) as orders, sum(net_revenue) as net_revenue
from analytics_db.marts.mart_daily_sales
group by 1, 2;
