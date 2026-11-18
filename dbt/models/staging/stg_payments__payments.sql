{# Semi-structured payments payload -> typed columns. Nested objects: fees, card, upi #}
with src as (
    select raw, _source_file, _file_row_number, _loaded_at
    from {{ source('raw', 'payments') }}
),

parsed as (
    select
        {{ json_str('raw', 'payment_id') }}                              as payment_id,
        {{ json_get('raw', 'order_id', dbt.type_int()) }}                as order_id,
        {{ json_get('raw', 'amount', 'decimal(12,2)') }}                 as amount,
        upper({{ json_str('raw', 'currency') }})                         as currency,
        {{ json_str('raw', 'method') }}                                  as payment_method,
        lower({{ json_str('raw', 'status') }})                           as payment_status,
        {{ iso_to_ts(json_str('raw', 'gateway_ts')) }}                   as gateway_ts,
        {{ json_str('raw', 'parent_payment_id') }}                       as parent_payment_id,
        {{ json_get('raw', 'fees.gateway_fee', 'decimal(12,2)') }}       as gateway_fee,
        {{ json_get('raw', 'fees.gst', 'decimal(12,2)') }}               as gateway_fee_gst,
        {{ json_str('raw', 'card.network') }}                            as card_network,
        {{ json_str('raw', 'upi.app') }}                                 as upi_app,
        _loaded_at,
        _source_file,
        _file_row_number
    from src
)

select * exclude (_source_file, _file_row_number)
from parsed
-- the extractor may re-deliver a payment after a retried page -> keep the latest load
qualify row_number() over (partition by payment_id order by _loaded_at desc, _source_file desc, _file_row_number desc) = 1
