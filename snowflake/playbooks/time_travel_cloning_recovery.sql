-- =====================================================================
-- 10  Recovery & environments: Time Travel, UNDROP, zero-copy clone
-- =====================================================================
use role transformer_role;

-- Bad dbt deploy overwrote a mart? Query it as of 1 hour ago...
select count(*) from analytics_db.marts.fct_orders at(offset => -3600);
-- ...or as of before a specific statement
select count(*) from analytics_db.marts.fct_orders before(statement => '<bad_query_id>');

-- Restore by cloning the old version and swapping (atomic)
create or replace table analytics_db.marts.fct_orders_restore clone analytics_db.marts.fct_orders at(offset => -3600);
alter table analytics_db.marts.fct_orders swap with analytics_db.marts.fct_orders_restore;

-- Accidentally dropped?
undrop table analytics_db.marts.mart_daily_sales;

-- Zero-copy clone for a dev / UAT environment or to test a migration on prod-sized data
create or replace database analytics_db_uat clone analytics_db;

-- Blue/green style deploy for a whole schema: build in MARTS_NEXT, validate, then swap
-- alter schema analytics_db.marts swap with analytics_db.marts_next;
