{# Explode the nested properties.items array of purchase events (LATERAL FLATTEN on Snowflake) #}
select
    e.event_id,
    e.order_id,
    e.event_ts,
    {{ json_get('it.value', 'product_id', dbt.type_int()) }}   as product_id,
    {{ json_get('it.value', 'qty', dbt.type_int()) }}          as quantity
from {{ ref('stg_clickstream__events') }} e
{{ flatten_json_array('e.raw', 'properties.items', 'it') }}
where e.event_type = 'purchase'
