# Power BI on the ShopStream marts

## Connect
* **Get Data → Snowflake**, server `<account>.snowflakecomputing.com`, warehouse `BI_WH`.
* Use the service user `SVC_POWERBI` (role `BI_READER_ROLE`): it can read `ANALYTICS_DB.MARTS` only.
  Customer email and phone come back **masked** because that role is not `PII_READER_ROLE`.
* Storage mode: **Import** for the dimensions and aggregated marts (fast, refreshed hourly after the
  pipeline); **DirectQuery** for `fct_orders` if near-real-time is required (composite model), with
  aggregations defined on `mart_daily_sales`.
* For on-premises gateways, add the gateway IP to the `np_service_users` network policy.

## Model (star schema)

| Table | Role | Relationship |
|---|---|---|
| `fct_orders` | fact, grain = order | `customer_sk` → `dim_customer` (many-to-one, single direction) · `order_date` → `dim_date[date_day]` |
| `fct_order_items` | fact, grain = order line | `product_sk` → `dim_product` · `order_date` → `dim_date` |
| `fct_payments` | fact, grain = payment event | `payment_date` → `dim_date` (inactive; activate with `USERELATIONSHIP`) |
| `dim_customer` | SCD2 dimension | filter "current only" with `is_current = TRUE` in visuals that need today's attributes |
| `dim_product`, `dim_date` | dimensions | mark `dim_date` as the date table |
| `mart_*` | pre-aggregated report tables | stand-alone pages (funnel, RFM, cohorts, reconciliation) |

## DAX measures

```DAX
Net Revenue      = SUM ( fct_orders[recognised_revenue] )
Orders Kept      = CALCULATE ( COUNTROWS ( fct_orders ), fct_orders[is_cancelled_or_returned] = FALSE () )
AOV              = DIVIDE ( [Net Revenue], [Orders Kept] )
Gross Margin     = SUM ( fct_orders[gross_margin] )
Gross Margin %   = DIVIDE ( [Gross Margin], [Net Revenue] )
Return Rate      = DIVIDE ( CALCULATE ( COUNTROWS ( fct_orders ), fct_orders[order_status] = "returned" ), COUNTROWS ( fct_orders ) )
Active Customers = CALCULATE ( DISTINCTCOUNT ( fct_orders[customer_id] ), fct_orders[is_cancelled_or_returned] = FALSE () )

Net Revenue PY   = CALCULATE ( [Net Revenue], SAMEPERIODLASTYEAR ( dim_date[date_day] ) )
Net Revenue YoY% = DIVIDE ( [Net Revenue] - [Net Revenue PY], [Net Revenue PY] )
Net Revenue FYTD = TOTALYTD ( [Net Revenue], dim_date[date_day], "31/3" )     -- Indian fiscal year
Net Revenue R28  = CALCULATE ( [Net Revenue], DATESINPERIOD ( dim_date[date_day], MAX ( dim_date[date_day] ), -28, DAY ) )

New Customer Orders = CALCULATE ( COUNTROWS ( fct_orders ), fct_orders[is_first_order] = TRUE () )
Repeat Order %      = 1 - DIVIDE ( [New Customer Orders], COUNTROWS ( fct_orders ) )

Captured Amount  = CALCULATE ( SUM ( fct_payments[amount] ), fct_payments[payment_status] = "captured",
                               USERELATIONSHIP ( fct_payments[payment_date], dim_date[date_day] ) )
Recon Exceptions = CALCULATE ( COUNTROWS ( mart_payment_reconciliation ),
                               mart_payment_reconciliation[recon_status] IN { "amount_mismatch", "missing_payment" } )
```

## Report pages
1. **Executive overview**: KPI cards (Net Revenue, YoY%, AOV, GM%, Active Customers, Return Rate), monthly revenue column chart with festive-season highlight, revenue by channel.
2. **Products**: category matrix (revenue, margin %, return rate), top-N products with a What-if parameter for N, price band slicer.
3. **Customers & marketing**: funnel visual from `mart_funnel_daily`, traffic-source table with conditional formatting on conversion, RFM segment bar, cohort retention matrix (cohort × months since first order, colour scale).
4. **Finance**: reconciliation status donut, exceptions table with drill-through to order details, payment method mix.
5. **Pipeline health**: `AUDIT.DQ_RESULTS` and `AUDIT.RECONCILIATION` (latest run), freshness card on `MAX(fct_orders[_dbt_loaded_at])`.

## Security & performance in Power BI
* **RLS** mirrors the Snowflake row access policy: role `South Managers` with filter
  `dim_customer[state] IN {"Telangana","Karnataka","Tamil Nadu","Kerala","Andhra Pradesh"}`.
* Hide surrogate keys and technical columns (`_changed_at`, `_dbt_loaded_at`).
* Keep visuals on the marts for heavy pages; use Performance Analyzer to confirm DirectQuery SQL hits the
  `order_date` clustering key (check `partitions_scanned` in `QUERY_HISTORY`).
* Schedule the dataset refresh 20 minutes after the Step Functions hourly run, or trigger it from the
  pipeline's success SNS via a Power Automate flow.

The interactive dashboard in this project (`bi/dashboard_index.html`, published as an artifact) shows the same pages.
