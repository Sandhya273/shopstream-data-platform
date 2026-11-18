-- =====================================================================
-- V1.0.1  Role-based access control
--   Functional roles (what people do) are granted access roles.
--   All custom roles roll up to SYSADMIN so admins keep visibility.
--
--   SYSADMIN
--     └── PLATFORM_ADMIN_ROLE
--           ├── LOADER_ROLE        (write RAW_DB)          <- SVC_LOADER, SVC_SNOWPIPE
--           ├── TRANSFORMER_ROLE   (read RAW, write ANALYTICS) <- SVC_DBT, engineers
--           │     └── ANALYST_ROLE (read ANALYTICS, ad hoc on BI_WH)
--           │           └── BI_READER_ROLE (read MARTS only) <- SVC_POWERBI
--           └── PII_READER_ROLE   (sees unmasked email/phone - granted case by case)
-- =====================================================================
use role securityadmin;

create role if not exists platform_admin_role;
create role if not exists loader_role;
create role if not exists transformer_role;
create role if not exists analyst_role;
create role if not exists bi_reader_role;
create role if not exists pii_reader_role;

grant role bi_reader_role   to role analyst_role;
grant role analyst_role     to role transformer_role;
grant role loader_role      to role platform_admin_role;
grant role transformer_role to role platform_admin_role;
grant role pii_reader_role  to role platform_admin_role;
grant role platform_admin_role to role sysadmin;

-- Warehouses
grant usage on warehouse load_wh      to role loader_role;
grant usage on warehouse transform_wh to role transformer_role;
grant usage on warehouse bi_wh        to role bi_reader_role;
grant operate, monitor on warehouse transform_wh to role platform_admin_role;

-- Databases are created in 02; grants below use FUTURE grants so new objects
-- created by dbt are readable without manual re-grants.
use role sysadmin;
create database if not exists raw_db;
create database if not exists analytics_db;
use role securityadmin;

grant usage on database raw_db to role loader_role;
grant usage on database raw_db to role transformer_role;
grant usage, create schema on database analytics_db to role transformer_role;
grant usage on database analytics_db to role bi_reader_role;

grant usage on all schemas in database raw_db to role transformer_role;
grant usage on future schemas in database raw_db to role transformer_role;
grant select on future tables in database raw_db to role transformer_role;
grant select on all tables in database raw_db to role transformer_role;

-- BI reader: MARTS only (not staging / intermediate)
grant usage on future schemas in database analytics_db to role analyst_role;
grant select on future tables in database analytics_db to role analyst_role;
grant select on future views  in database analytics_db to role analyst_role;

-- ---------------------------------------------------------------------
-- Service users: key-pair auth, no passwords, default roles/warehouses
-- (public keys are rotated by the platform team; RSA_PUBLIC_KEY_2 used for zero-downtime rotation)
-- ---------------------------------------------------------------------
use role useradmin;
create user if not exists svc_loader  type = service default_role = loader_role      default_warehouse = load_wh      rsa_public_key = '<LOADER_PUBLIC_KEY>';
create user if not exists svc_dbt     type = service default_role = transformer_role default_warehouse = transform_wh rsa_public_key = '<DBT_PUBLIC_KEY>';
create user if not exists svc_powerbi type = service default_role = bi_reader_role   default_warehouse = bi_wh        rsa_public_key = '<POWERBI_PUBLIC_KEY>';

use role securityadmin;
grant role loader_role      to user svc_loader;
grant role transformer_role to user svc_dbt;
grant role bi_reader_role   to user svc_powerbi;

-- Network policy: service users only from the VPC NAT / Power BI gateway IPs
create network policy if not exists np_service_users allowed_ip_list = ('10.20.0.0/16', '52.0.0.10');
alter user svc_dbt     set network_policy = np_service_users;
alter user svc_loader  set network_policy = np_service_users;
alter user svc_powerbi set network_policy = np_service_users;
