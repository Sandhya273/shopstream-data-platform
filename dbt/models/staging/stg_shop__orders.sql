{#
  Current state of each order from the DMS CDC stream.
  - exact duplicates from DMS task restarts are collapsed
  - latest change per order wins (cdc_ts, then D > U > I, then file position)
  - hard-deleted orders (Op = 'D') are kept with is_deleted = true (audit), filtered in marts
#}
with cdc as (
    select
        op,
        {{ dbt.safe_cast('cdc_ts', dbt.type_timestamp()) }}              as cdc_ts,
        {{ dbt.safe_cast('order_id', dbt.type_int()) }}                  as order_id,
        {{ dbt.safe_cast('customer_id', dbt.type_int()) }}               as customer_id,
        {{ dbt.safe_cast('order_ts', dbt.type_timestamp()) }}            as order_ts,
        lower(trim(order_status))                                        as order_status,
        lower(trim(channel))                                             as channel,
        nullif(trim(shipping_city), '')                                  as shipping_city,
        nullif(upper(trim(promo_code)), '')                              as promo_code,
        {{ dbt.safe_cast("nullif(shipping_fee, '')", 'decimal(12,2)') }} as shipping_fee,
        {{ dbt.safe_cast('updated_at', dbt.type_timestamp()) }}          as updated_at,
        _source_file,
        _file_row_number
    from {{ source('raw', 'dms_orders') }}
),

latest as (
    select *
    from cdc
    qualify row_number() over (
        partition by order_id
        order by cdc_ts desc,
                 case op when 'D' then 3 when 'U' then 2 else 1 end desc,
                 _source_file desc, _file_row_number desc
    ) = 1
)

select
    order_id,
    customer_id,
    order_ts,
    cast(order_ts as date)                                  as order_date,
    order_status,
    channel,
    shipping_city,
    promo_code,
    shipping_fee,
    updated_at,
    cdc_ts                                                  as last_cdc_ts,
    op = 'D'                                                as is_deleted
from latest
