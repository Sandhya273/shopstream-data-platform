with orders as (
    select * from {{ ref('stg_shop__orders') }} where not is_deleted
),

items as (
    select
        i.order_id,
        count(*)                                  as line_count,
        sum(i.quantity)                           as units,
        sum(i.gross_amount)                       as gross_amount,
        sum(i.discount_amount)                    as discount_amount,
        sum(i.net_amount)                         as net_item_amount,
        sum(i.quantity * p.unit_cost)             as cogs_amount
    from {{ ref('stg_shop__order_items') }} i
    left join {{ ref('stg_shop__products') }} p using (product_id)
    group by 1
),

pay as (
    select * from {{ ref('int_order_payments') }}
)

select
    o.order_id,
    o.customer_id,
    o.order_ts,
    o.order_date,
    o.order_status,
    o.channel,
    o.shipping_city,
    o.promo_code,
    coalesce(i.line_count, 0)                                   as line_count,
    coalesce(i.units, 0)                                        as units,
    coalesce(i.gross_amount, 0)                                 as gross_amount,
    coalesce(i.discount_amount, 0)                              as discount_amount,
    coalesce(i.net_item_amount, 0)                              as net_item_amount,
    o.shipping_fee,
    coalesce(i.net_item_amount, 0) + coalesce(o.shipping_fee, 0) as order_total,
    coalesce(i.cogs_amount, 0)                                  as cogs_amount,
    coalesce(p.captured_amount, 0)                              as captured_amount,
    coalesce(p.refunded_amount, 0)                              as refunded_amount,
    coalesce(p.failed_attempts, 0)                              as failed_payment_attempts,
    coalesce(p.gateway_fees, 0)                                 as gateway_fees,
    p.payment_method,
    p.captured_at,
    o.order_status in ('cancelled', 'returned')                 as is_cancelled_or_returned,
    -- revenue is recognised for orders that were not cancelled/returned
    case when o.order_status in ('cancelled', 'returned') then 0
         else coalesce(i.net_item_amount, 0) end               as recognised_revenue,
    row_number() over (partition by o.customer_id order by o.order_ts, o.order_id) as customer_order_seq,
    o.updated_at,
    greatest(o.updated_at, coalesce(p.last_payment_event_at, o.updated_at)) as _changed_at
from orders o
left join items i using (order_id)
left join pay p using (order_id)
