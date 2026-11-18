{# Calendar dimension incl. Indian fiscal year (Apr-Mar) and festive-season flag #}
with spine as (
    {{ dbt.date_spine('day', "cast('2024-01-01' as date)", "cast('2028-01-01' as date)") }}
),
d as (
    select cast(date_day as date) as date_day from spine
)
select
    cast({{ dbt.date_trunc('day', 'date_day') }} as date)                         as date_day,
    cast(replace(cast(date_day as {{ dbt.type_string() }}), '-', '') as integer)  as date_key,
    extract(year from date_day)                                                   as year,
    extract(quarter from date_day)                                                as quarter,
    extract(month from date_day)                                                  as month,
    extract(day from date_day)                                                    as day_of_month,
    extract(dow from date_day)                                                    as day_of_week,      -- 0 = Sunday
    case extract(dow from date_day) when 0 then 'Sun' when 1 then 'Mon' when 2 then 'Tue'
         when 3 then 'Wed' when 4 then 'Thu' when 5 then 'Fri' else 'Sat' end      as day_name,
    extract(dow from date_day) in (0, 6)                                          as is_weekend,
    cast({{ dbt.date_trunc('month', 'date_day') }} as date)                       as month_start,
    cast({{ dbt.date_trunc('week', 'date_day') }} as date)                        as week_start,
    case when extract(month from date_day) >= 4 then extract(year from date_day) + 1
         else extract(year from date_day) end                                     as fiscal_year,      -- FY ends 31-Mar
    case when extract(month from date_day) between 4 and 6 then 1
         when extract(month from date_day) between 7 and 9 then 2
         when extract(month from date_day) between 10 and 12 then 3 else 4 end    as fiscal_quarter,
    extract(month from date_day) in (10, 11)                                      as is_festive_season
from d
