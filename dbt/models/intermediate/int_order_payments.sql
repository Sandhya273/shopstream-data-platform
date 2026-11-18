{# Payment activity rolled up to one row per order #}
select
    order_id,
    sum(case when payment_status = 'captured' then amount else 0 end)      as captured_amount,
    sum(case when payment_status = 'refunded' then amount else 0 end)      as refunded_amount,
    sum(case when payment_status = 'failed' then 1 else 0 end)             as failed_attempts,
    sum(case when payment_status = 'captured' then gateway_fee + gateway_fee_gst else 0 end) as gateway_fees,
    max(case when payment_status = 'captured' then payment_method end)     as payment_method,
    min(case when payment_status = 'captured' then gateway_ts end)         as captured_at,
    max(gateway_ts)                                                        as last_payment_event_at,
    count(*)                                                               as payment_events
from {{ ref('stg_payments__payments') }}
group by 1
