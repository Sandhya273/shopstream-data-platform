{{
  config(
    materialized = 'incremental',
    unique_key = 'session_id',
    incremental_strategy = incremental_strategy(),
    cluster_by = ['session_date']
  )
}}
{# Near-real-time model: run every 15 minutes by the clickstream task graph. Late events are absorbed by the lookback. #}
select
    s.*,
    c.customer_sk
from {{ ref('int_sessions') }} s
left join {{ ref('int_customers_scd2') }} c
       on c.customer_id = s.customer_id
      and s.session_start_ts >= c.valid_from
      and s.session_start_ts <  c.valid_to
{% if is_incremental() %}
where s.session_date >= (
    select {{ dbt.dateadd('day', -var('incremental_lookback_days'), 'max(session_date)') }} from {{ this }}
)
{% endif %}
