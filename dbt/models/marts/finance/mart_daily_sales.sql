{# Daily executive KPIs by channel #}
select
    f.order_date,
    d.month_start,
    d.fiscal_year,
    d.is_festive_season,
    f.channel,
    count(*)                                                        as orders,
    sum(case when f.is_first_order then 1 else 0 end)               as new_customer_orders,
    count(distinct f.customer_id)                                   as customers,
    sum(f.units)                                                    as units,
    sum(f.gross_amount)                                             as gross_sales,
    sum(f.discount_amount)                                          as discounts,
    sum(f.recognised_revenue)                                       as net_revenue,
    sum(case when f.is_cancelled_or_returned then 0 else f.cogs_amount end) as cogs,
    sum(f.gross_margin)                                             as gross_margin,
    sum(case when f.order_status = 'cancelled' then 1 else 0 end)   as cancelled_orders,
    sum(case when f.order_status = 'returned' then 1 else 0 end)    as returned_orders,
    sum(f.gateway_fees)                                             as gateway_fees
from {{ ref('fct_orders') }} f
join {{ ref('dim_date') }} d on d.date_day = f.order_date
group by 1, 2, 3, 4, 5
