select
    {{ surrogate_key(['product_id']) }}                       as product_sk,
    product_id,
    sku,
    product_name,
    category,
    sub_category,
    brand,
    unit_price                                                as current_unit_price,
    unit_cost                                                 as current_unit_cost,
    round((unit_price - unit_cost) / nullif(unit_price, 0), 4) as current_margin_pct,
    case when unit_price < 1000 then '< 1K'
         when unit_price < 5000 then '1K - 5K'
         when unit_price < 20000 then '5K - 20K'
         else '20K +' end                                     as price_band,
    is_active,
    updated_at
from {{ ref('stg_shop__products') }}
