-- =====================================================================
-- V1.1.0  Data governance: tags, dynamic masking, row access, auditing
-- =====================================================================
use role sysadmin;
create schema if not exists analytics_db.governance;
use schema analytics_db.governance;

-- ---- Object tagging (classification) ----
create tag if not exists pii_type allowed_values 'email', 'phone', 'name', 'none';
create tag if not exists data_domain allowed_values 'sales', 'finance', 'marketing', 'customer';

-- ---- Masking policies: PII visible only to PII_READER_ROLE ----
create masking policy if not exists pii_email_mask as (val string) returns string ->
  case
    when is_role_in_session('PII_READER_ROLE') then val
    when val is null then null
    else regexp_replace(val, '^[^@]+', '*****')          -- *****@example.com keeps domain for analysis
  end;

create masking policy if not exists pii_phone_mask as (val string) returns string ->
  case
    when is_role_in_session('PII_READER_ROLE') then val
    else concat('XXXXXX', right(val, 4))
  end;


-- NOTE: column-level attachments live in the dbt model (post_hook on dim_customer),
-- because dbt re-creates tables and CREATE OR REPLACE drops column policies/tags.

-- ---- Row access policy: regional managers only see their states ----
create table if not exists analytics_db.governance.region_access_map (role_name varchar, state varchar);
insert into analytics_db.governance.region_access_map values
  ('SOUTH_MANAGER_ROLE', 'Telangana'), ('SOUTH_MANAGER_ROLE', 'Karnataka'),
  ('SOUTH_MANAGER_ROLE', 'Tamil Nadu'), ('SOUTH_MANAGER_ROLE', 'Kerala'),
  ('SOUTH_MANAGER_ROLE', 'Andhra Pradesh');

create row access policy if not exists rap_customer_region as (customer_state varchar) returns boolean ->
  is_role_in_session('ANALYST_ROLE')            -- analysts & above see everything
  or exists (
      select 1 from analytics_db.governance.region_access_map m
      where is_role_in_session(m.role_name) and m.state = customer_state
  );

-- attached in dim_customer post_hook (see note above)

