{#
  SCD Type 2 customer history built directly from the CDC change log.
  Advantage over a dbt snapshot: history is complete even for changes that
  happened between two pipeline runs, and the model is fully rebuildable.
#}
with versions as (
    select *
    from {{ ref('stg_shop__customers_history') }}
    where op <> 'D'
)

select
    {{ surrogate_key(['customer_id', 'cdc_ts']) }}                         as customer_sk,
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    city,
    state,
    country,
    segment,
    signup_ts,
    cdc_ts                                                                 as valid_from,
    coalesce(
        lead(cdc_ts) over (partition by customer_id order by cdc_ts),
        cast('9999-12-31 00:00:00' as {{ dbt.type_timestamp() }})
    )                                                                      as valid_to,
    lead(cdc_ts) over (partition by customer_id order by cdc_ts) is null   as is_current,
    row_number() over (partition by customer_id order by cdc_ts)           as version_number
from versions
