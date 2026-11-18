{#
  dbt snapshot (SCD2) of the product catalogue - captures price changes run-to-run.
  (Customers use CDC-derived SCD2 in int_customers_scd2 instead; both patterns shown.)
#}
{% snapshot products_snapshot %}
{{
  config(
    unique_key = 'product_id',
    strategy = 'check',
    check_cols = ['unit_price', 'unit_cost', 'is_active', 'category'],
    invalidate_hard_deletes = True
  )
}}
select * from {{ ref('stg_shop__products') }}
{% endsnapshot %}
