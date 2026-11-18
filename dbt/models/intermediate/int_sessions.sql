{# Clickstream sessionised: one row per session with funnel flags #}
with ev as (
    select * from {{ ref('stg_clickstream__events') }}
),

first_touch as (
    select session_id, device_type, device_os, utm_source, utm_medium, utm_campaign, referrer
    from ev
    qualify row_number() over (partition by session_id order by event_ts, event_id) = 1
)

select
    e.session_id,
    max(e.customer_id)                                                         as customer_id,
    min(e.event_ts)                                                            as session_start_ts,
    cast(min(e.event_ts) as date)                                              as session_date,
    max(e.event_ts)                                                            as session_end_ts,
    {{ dbt.datediff('min(e.event_ts)', 'max(e.event_ts)', 'second') }}         as session_duration_s,
    count(*)                                                                   as events,
    sum(case when e.event_type = 'product_view' then 1 else 0 end)             as product_views,
    max(case when e.event_type = 'product_view' then 1 else 0 end)             as reached_product_view,
    max(case when e.event_type = 'add_to_cart' then 1 else 0 end)              as reached_add_to_cart,
    max(case when e.event_type = 'checkout_start' then 1 else 0 end)           as reached_checkout,
    max(case when e.event_type = 'purchase' then 1 else 0 end)                 as converted,
    max(e.order_id)                                                            as order_id,
    coalesce(sum(e.purchase_revenue), 0)                                       as purchase_revenue,
    max(e.customer_id) is null                                                 as is_anonymous,
    f.device_type, f.device_os, f.utm_source, f.utm_medium, f.utm_campaign, f.referrer,
    max(e._loaded_at)                                                          as _loaded_at
from ev e
join first_touch f using (session_id)
group by e.session_id, f.device_type, f.device_os, f.utm_source, f.utm_medium, f.utm_campaign, f.referrer
