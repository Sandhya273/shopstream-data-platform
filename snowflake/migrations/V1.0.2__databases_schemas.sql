-- =====================================================================
-- V1.0.2  Databases & schemas (medallion-style layering)
--   RAW_DB.RAW          bronze : files as delivered, all metadata kept
--   RAW_DB.AUDIT        load/run audit, DQ results
--   ANALYTICS_DB.STAGING      silver: typed, deduplicated (dbt views)
--   ANALYTICS_DB.INTERMEDIATE silver: business logic building blocks (transient)
--   ANALYTICS_DB.MARTS        gold  : star schema + reporting marts
--   ANALYTICS_DB.SNAPSHOTS    dbt snapshots (SCD2)
-- =====================================================================
use role sysadmin;

create database if not exists raw_db       data_retention_time_in_days = 7  comment = 'Bronze - landed source data';
create database if not exists analytics_db data_retention_time_in_days = 3  comment = 'Silver/Gold - dbt managed';

create schema if not exists raw_db.raw   with managed access;
create schema if not exists raw_db.audit with managed access;

-- transient = no fail-safe (rebuildable by dbt) -> lower storage cost
create transient schema if not exists analytics_db.staging;
create transient schema if not exists analytics_db.intermediate;
create schema if not exists analytics_db.marts data_retention_time_in_days = 14;  -- longer Time Travel on gold
create schema if not exists analytics_db.snapshots data_retention_time_in_days = 30;

grant ownership on schema analytics_db.staging      to role transformer_role copy current grants;
grant ownership on schema analytics_db.intermediate to role transformer_role copy current grants;
grant ownership on schema analytics_db.marts        to role transformer_role copy current grants;
grant ownership on schema analytics_db.snapshots    to role transformer_role copy current grants;
grant usage, create table, create view on schema raw_db.raw   to role loader_role;
grant usage, create table             on schema raw_db.audit to role loader_role;

-- BI readers only see MARTS
grant usage on schema analytics_db.marts to role bi_reader_role;
grant select on future tables in schema analytics_db.marts to role bi_reader_role;
grant select on future views  in schema analytics_db.marts to role bi_reader_role;

-- Dev environments: zero-copy clone of prod in seconds, no extra storage until data changes
-- create database analytics_db_dev clone analytics_db;
