{{
  config(
    post_hook = [
      "{% if target.type == 'snowflake' %}alter table {{ this }} modify column email set masking policy analytics_db.governance.pii_email_mask, column phone set masking policy analytics_db.governance.pii_phone_mask{% endif %}",
      "{% if target.type == 'snowflake' %}alter table {{ this }} set tag analytics_db.governance.data_domain = 'customer'{% endif %}",
      "{% if target.type == 'snowflake' %}alter table {{ this }} add row access policy analytics_db.governance.rap_customer_region on (state){% endif %}"
    ]
  )
}}
{#
  SCD2 customer dimension.
  Snowflake: the post-hooks re-attach masking + row access policies on every build, because
  dbt re-creates the table and CREATE OR REPLACE drops column-level policies.
  Policies are defined in snowflake/migrations/V1.1.0__security_governance.sql.
#}
select
    customer_sk,
    customer_id,
    first_name,
    last_name,
    first_name || ' ' || last_name     as full_name,
    email,
    phone,
    city,
    state,
    country,
    segment,
    signup_ts,
    cast(signup_ts as date)            as signup_date,
    valid_from,
    valid_to,
    is_current,
    version_number
from {{ ref('int_customers_scd2') }}
