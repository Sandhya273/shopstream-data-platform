select
    p.payment_id,
    p.order_id,
    f.customer_sk,
    cast(p.gateway_ts as date)       as payment_date,
    p.gateway_ts,
    p.payment_method,
    p.payment_status,
    p.amount,
    p.currency,
    p.gateway_fee,
    p.gateway_fee_gst,
    p.card_network,
    p.upi_app,
    p.parent_payment_id,
    f.order_id is not null           as has_matching_order
from {{ ref('stg_payments__payments') }} p
left join {{ ref('fct_orders') }} f using (order_id)
