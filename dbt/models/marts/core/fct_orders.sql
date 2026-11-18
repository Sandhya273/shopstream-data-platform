{{
  config(
    materialized = 'incremental',
    unique_key = 'order_id',
    incremental_strategy = incremental_strategy(),
    on_schema_change = 'append_new_columns',
    cluster_by = ['order_date'],
    post_hook = [
      "delete from {{ this }} where order_id in (select order_id from {{ ref('stg_shop__orders') }} where is_deleted)"
    ]
  )
}}
{#
  Grain: one row per order.
  Incremental: re-processes orders changed (status / payment / refund) since the
  last load minus a lookback window, then MERGEs on order_id. Hard deletes in the
  source are propagated by the post-hook.
  customer_sk is resolved point-in-time: the customer version valid when the order was placed.
#}
with o as (
    select * from {{ ref('int_orders_enriched') }}
    {% if is_incremental() %}
    where _changed_at >= (
        select {{ dbt.dateadd('day', -var('incremental_lookback_days'), 'max(_changed_at)') }} from {{ this }}
    )
    {% endif %}
)

select
    o.order_id,
    c.customer_sk,
    o.customer_id,
    cast(replace(cast(o.order_date as {{ dbt.type_string() }}), '-', '') as integer) as order_date_key,
    o.order_date,
    o.order_ts,
    o.order_status,
    o.channel,
    o.shipping_city,
    o.promo_code,
    o.payment_method,
    o.line_count,
    o.units,
    o.gross_amount,
    o.discount_amount,
    o.net_item_amount,
    o.shipping_fee,
    o.order_total,
    o.cogs_amount,
    o.recognised_revenue,
    case when o.is_cancelled_or_returned then 0 else o.net_item_amount - o.cogs_amount end as gross_margin,
    o.captured_amount,
    o.refunded_amount,
    o.gateway_fees,
    o.failed_payment_attempts,
    o.is_cancelled_or_returned,
    o.customer_order_seq,
    o.customer_order_seq = 1                       as is_first_order,
    o.captured_at,
    o.updated_at,
    o._changed_at,
    {{ dbt.current_timestamp() }}                  as _dbt_loaded_at
from o
left join {{ ref('int_customers_scd2') }} c
       on c.customer_id = o.customer_id
      and o.order_ts >= c.valid_from
      and o.order_ts <  c.valid_to
