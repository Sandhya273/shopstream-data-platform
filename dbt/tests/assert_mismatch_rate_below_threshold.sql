-- Finance guardrail: fail the build if > 1% of orders do not reconcile with the payment gateway
{{ config(severity = 'error') }}
select
    sum(case when recon_status in ('amount_mismatch', 'missing_payment') then 1 else 0 end) * 1.0 / count(*) as exception_rate
from {{ ref('mart_payment_reconciliation') }}
having sum(case when recon_status in ('amount_mismatch', 'missing_payment') then 1 else 0 end) * 1.0 / count(*) > 0.01
