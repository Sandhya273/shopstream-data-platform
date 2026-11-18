{#
  Order-to-cash reconciliation: what the shop says the customer owes vs what the
  gateway captured / refunded. Feeds the finance exceptions report and DQ alerts.
#}
with o as (
    select * from {{ ref('fct_orders') }}
)
select
    o.order_id,
    o.order_date,
    o.order_status,
    o.payment_method,
    o.order_total,
    o.captured_amount,
    o.refunded_amount,
    o.captured_amount - o.order_total                                     as capture_variance,
    case
        when o.order_status = 'cancelled' and o.captured_amount = 0                         then 'not_charged'
        when o.captured_amount = 0 and o.order_status in ('placed', 'shipped')              then 'pending_cod_or_capture'
        when o.captured_amount = 0                                                          then 'missing_payment'
        when abs(o.captured_amount - o.order_total) > 0.01                                  then 'amount_mismatch'
        when o.order_status in ('cancelled', 'returned') and o.payment_method <> 'COD'
             and o.refunded_amount < o.captured_amount                                      then 'refund_pending'
        else 'matched'
    end                                                                   as recon_status
from o
