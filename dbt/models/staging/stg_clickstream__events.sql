{# One row per unique event. Cross-file duplicates (Firehose at-least-once) removed here. #}
with src as (
    select raw, _source_file, _loaded_at
    from {{ source('raw', 'clickstream_events') }}
)

select
    {{ json_str('raw', 'event_id') }}                                  as event_id,
    {{ json_str('raw', 'event_type') }}                                as event_type,
    {{ iso_to_ts(json_str('raw', 'event_ts')) }}                       as event_ts,
    cast({{ iso_to_ts(json_str('raw', 'event_ts')) }} as date)         as event_date,
    {{ json_str('raw', 'session_id') }}                                as session_id,
    {{ json_get('raw', 'customer_id', dbt.type_int()) }}               as customer_id,
    {{ json_str('raw', 'device.type') }}                               as device_type,
    {{ json_str('raw', 'device.os') }}                                 as device_os,
    {{ json_str('raw', 'page.url') }}                                  as page_url,
    {{ json_str('raw', 'page.referrer') }}                             as referrer,
    coalesce({{ json_str('raw', 'utm.source') }}, 'direct')            as utm_source,
    coalesce({{ json_str('raw', 'utm.medium') }}, 'none')              as utm_medium,
    {{ json_str('raw', 'utm.campaign') }}                              as utm_campaign,
    {{ json_get('raw', 'properties.product_id', dbt.type_int()) }}     as product_id,
    {{ json_get('raw', 'properties.order_id', dbt.type_int()) }}       as order_id,
    {{ json_get('raw', 'properties.revenue', 'decimal(12,2)') }}       as purchase_revenue,
    raw,
    _source_file,
    _loaded_at
from src
qualify row_number() over (partition by {{ json_str('raw', 'event_id') }} order by _loaded_at, _source_file) = 1
