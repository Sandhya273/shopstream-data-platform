{# Conversion funnel by day x device x traffic source #}
select
    session_date,
    device_type,
    utm_source,
    utm_medium,
    count(*)                                         as sessions,
    sum(case when is_anonymous then 1 else 0 end)    as anonymous_sessions,
    sum(reached_product_view)                        as product_view_sessions,
    sum(reached_add_to_cart)                         as add_to_cart_sessions,
    sum(reached_checkout)                            as checkout_sessions,
    sum(converted)                                   as purchase_sessions,
    sum(purchase_revenue)                            as purchase_revenue,
    round(sum(converted) * 1.0 / nullif(count(*), 0), 4)                          as conversion_rate,
    round(sum(reached_add_to_cart) * 1.0 / nullif(sum(reached_product_view), 0), 4) as view_to_cart_rate,
    round(sum(converted) * 1.0 / nullif(sum(reached_checkout), 0), 4)              as checkout_completion_rate,
    round(avg(session_duration_s), 1)                                              as avg_session_duration_s
from {{ ref('fct_sessions') }}
group by 1, 2, 3, 4
