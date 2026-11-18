select
    p.product_id,
    p.product_name,
    p.category,
    p.sub_category,
    p.brand,
    p.price_band,
    count(distinct i.order_id)                                                         as orders,
    sum(i.quantity)                                                                    as units_sold,
    sum(case when i.is_cancelled_or_returned then 0 else i.net_amount end)             as net_revenue,
    sum(case when i.is_cancelled_or_returned then 0 else i.gross_margin end)           as gross_margin,
    round(sum(case when i.order_status = 'returned' then i.quantity else 0 end) * 1.0
          / nullif(sum(i.quantity), 0), 4)                                             as return_rate,
    rank() over (partition by p.category order by sum(case when i.is_cancelled_or_returned then 0 else i.net_amount end) desc) as rank_in_category
from {{ ref('fct_order_items') }} i
join {{ ref('dim_product') }} p using (product_id)
group by 1, 2, 3, 4, 5, 6
