{{
  config(
    materialized = 'incremental',
    unique_key = 'order_item_id',
    incremental_strategy = incremental_strategy(),
    on_schema_change = 'append_new_columns',
    cluster_by = ['order_date']
  )
}}
{# Grain: one row per order line. Joins to fct_orders for status and the point-in-time customer key. #}
select
    i.order_item_id,
    i.order_id,
    f.customer_sk,
    p.product_sk,
    i.product_id,
    f.order_date,
    f.order_date_key,
    f.order_status,
    f.channel,
    i.quantity,
    i.unit_price,
    i.gross_amount,
    i.discount_amount,
    i.net_amount,
    i.quantity * p.current_unit_cost                                  as cogs_amount,
    i.net_amount - i.quantity * p.current_unit_cost                   as gross_margin,
    f.is_cancelled_or_returned,
    f._changed_at
from {{ ref('stg_shop__order_items') }} i
join {{ ref('fct_orders') }} f using (order_id)
left join {{ ref('dim_product') }} p using (product_id)
{% if is_incremental() %}
where f._changed_at >= (
    select {{ dbt.dateadd('day', -var('incremental_lookback_days'), 'max(_changed_at)') }} from {{ this }}
)
{% endif %}
