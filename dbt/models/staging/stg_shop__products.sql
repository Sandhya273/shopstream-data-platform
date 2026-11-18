with cdc as (
    select
        op,
        {{ dbt.safe_cast('cdc_ts', dbt.type_timestamp()) }}   as cdc_ts,
        {{ dbt.safe_cast('product_id', dbt.type_int()) }}     as product_id,
        trim(sku)                                             as sku,
        trim(product_name)                                    as product_name,
        trim(category)                                        as category,
        trim(sub_category)                                    as sub_category,
        trim(brand)                                           as brand,
        {{ dbt.safe_cast('unit_price', 'decimal(12,2)') }}    as unit_price,
        {{ dbt.safe_cast('unit_cost', 'decimal(12,2)') }}     as unit_cost,
        lower(is_active) in ('true', 't', '1')                as is_active,
        {{ dbt.safe_cast('updated_at', dbt.type_timestamp()) }} as updated_at,
        _source_file, _file_row_number
    from {{ source('raw', 'dms_products') }}
)
select product_id, sku, product_name, category, sub_category, brand,
       unit_price, unit_cost, is_active, updated_at
from cdc
qualify row_number() over (partition by product_id order by cdc_ts desc, _source_file desc, _file_row_number desc) = 1
    and op <> 'D'
