{# Every version of every customer (one row per distinct CDC change) - feeds SCD2 #}
with cdc as (
    select
        op,
        {{ dbt.safe_cast('cdc_ts', dbt.type_timestamp()) }}      as cdc_ts,
        {{ dbt.safe_cast('customer_id', dbt.type_int()) }}       as customer_id,
        trim(first_name)                                as first_name,
        trim(last_name)                                 as last_name,
        nullif(lower(trim(email)), '')                           as email,
        nullif(trim(phone), '')                                  as phone,
        trim(city)                                               as city,
        trim(state)                                              as state,
        trim(country)                                            as country,
        lower(trim(segment))                                     as segment,
        {{ dbt.safe_cast('signup_ts', dbt.type_timestamp()) }}   as signup_ts,
        _source_file, _file_row_number
    from {{ source('raw', 'dms_customers') }}
)
select *
from cdc
qualify row_number() over (partition by customer_id, cdc_ts order by _source_file desc, _file_row_number desc) = 1
