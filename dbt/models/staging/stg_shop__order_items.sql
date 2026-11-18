with cdc as (
    select
        op,
        {{ dbt.safe_cast('cdc_ts', dbt.type_timestamp()) }}               as cdc_ts,
        {{ dbt.safe_cast('order_item_id', dbt.type_int()) }}              as order_item_id,
        {{ dbt.safe_cast('order_id', dbt.type_int()) }}                   as order_id,
        {{ dbt.safe_cast('product_id', dbt.type_int()) }}                 as product_id,
        {{ dbt.safe_cast('quantity', dbt.type_int()) }}                   as quantity,
        {{ dbt.safe_cast('unit_price', 'decimal(12,2)') }}                as unit_price,
        {{ dbt.safe_cast('discount_amount', 'decimal(12,2)') }}           as discount_amount,
        {{ dbt.safe_cast('updated_at', dbt.type_timestamp()) }}           as updated_at,
        _source_file, _file_row_number
    from {{ source('raw', 'dms_order_items') }}
)

select
    order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    discount_amount,
    quantity * unit_price                       as gross_amount,
    quantity * unit_price - discount_amount     as net_amount,
    updated_at
from cdc
qualify row_number() over (partition by order_item_id order by cdc_ts desc, _source_file desc, _file_row_number desc) = 1
    and op <> 'D'
