{# Current version of each customer #}
select
    customer_id, first_name, last_name, email,
    {{ regex_like('email', '^[^@ ]+@[^@ ]+[.][^@ ]+$') }}   as is_email_valid,
    phone, city, state, country, segment, signup_ts,
    cdc_ts                                                   as updated_at
from {{ ref('stg_shop__customers_history') }}
qualify row_number() over (partition by customer_id order by cdc_ts desc) = 1
    and op <> 'D'
