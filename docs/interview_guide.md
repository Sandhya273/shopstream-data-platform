# Interview guide: walking through ShopStream

How to present the project and answer the questions that usually follow. Each answer points at the file
that proves it.

## The 2-minute pitch

"I built an end-to-end retail data platform on Snowflake and AWS. Three sources feed it: an OLTP Postgres
database through AWS DMS change data capture, a payments REST API, and web/app clickstream. Raw files land
in S3. A Lambda validates and quarantines bad clickstream events, and Snowpipe loads them continuously.
Batch loads run hourly from a Step Functions pipeline that EventBridge schedules: API extraction, Glue
compaction, COPY INTO, dbt on ECS Fargate, then a data quality and reconciliation stage that alerts through
SNS. In Snowflake I modelled a star schema with dbt. The customer dimension is SCD2, built from the CDC
log, and the facts are incremental MERGE models. I added masking and row access policies, separate
warehouses with resource monitors, and clustering for performance. On top sit Power BI reports for sales,
products, customers, payment reconciliation and pipeline health. Everything is in Terraform and GitHub
Actions, and the whole pipeline runs locally on DuckDB so CI can test it end to end."

## Mapping to the job description

| JD requirement | Show this |
|---|---|
| Micro-partitioning, pruning, clustering | `snowflake/playbooks/performance_tuning.sql` §A; `cluster_by` in `fct_orders.sql` |
| Workload mgmt, concurrency, warehouse optimisation | `V1.0.0__warehouses_resource_monitors.sql` (per-workload WHs, multi-cluster BI_WH, QAS) |
| Credit consumption | resource monitors + `performance_tuning.sql` §E (metering, cost per dbt model, serverless) |
| Query performance troubleshooting | `performance_tuning.sql` §C (spill, queueing, scan ratio, `GET_QUERY_OPERATOR_STATS`) |
| Semi-structured data | VARIANT payments/clickstream, `json_str` macro, `LATERAL FLATTEN` (`stg_clickstream__purchase_items`, task graph) |
| Security, governance, access | `V1.0.1__rbac.sql`, `V1.1.0__security_governance.sql`, `dim_customer` post-hooks |
| Advanced features / automation | Snowpipe, Streams, Tasks (serverless, finalizer), Dynamic Table, DMFs, Alerts, zero-copy clone, Time Travel |
| Joins, CTEs, analytical functions | `QUALIFY ROW_NUMBER`, `LEAD` (SCD2), `NTILE` (RFM), `RANK`, cohort `datediff` |
| Python: APIs, JSON, CSV, Parquet, logging, exceptions | `payments_extractor.py`, `handler.py`, `logging_utils.py`, `load_raw_local.py` |
| AWS S3/Glue/Lambda/DMS/IAM/EventBridge/Step Functions | `infra/terraform/*.tf`, `orchestration/step_functions/` |
| Batch + near-real-time | hourly Step Functions + 5-minute stream/task graph + Snowpipe |
| Incremental processing & sync | CDC dedupe, watermark extractor, incremental MERGE with lookback, delete propagation |
| Validation / reconciliation framework | `data_quality/dq_framework.py`, `reconciliation.py`, dbt tests |
| Migration & modernisation | DMS full-load + CDC from OLTP, schemachange-versioned DDL, clone-based UAT |
| Good to have: dbt, Snowpark, Git/CI/CD, Docker, Terraform, Power BI | all present (`streamlit_app.py` uses Snowpark) |

## Likely questions, short answers

**How do you deduplicate CDC data?** I keep the latest record per key: `QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY cdc_ts DESC, op priority, file, row)`.
Exact replays collapse, and a delete that shares a timestamp with an update wins. Deleted keys stay
flagged in staging for audit and are removed from the fact by a post-hook.

**SCD2: snapshot or CDC?** Both are in the repo. A dbt snapshot only sees the state at run time, so two
changes between runs lose one of them. Building from the CDC log captures every version and can be rebuilt
at any time. Facts join on `order_ts >= valid_from AND order_ts < valid_to`.

**How do incremental models handle late data?** Each run reprocesses rows whose `_changed_at` falls within
the last 3 days and MERGEs on the key. `_changed_at` includes the payment timestamp, so a refund that
arrives later still updates the order.

**Why does the fact table use clustering?** Most queries filter by date. Clustering on `order_date` keeps
each micro-partition's min/max date range narrow, so a filter on one month prunes most partitions. I check
this with `SYSTEM$CLUSTERING_INFORMATION` and partitions scanned vs total. I wouldn't cluster small tables,
because automatic clustering costs credits.

**A query got slow. What do you do?** Query profile first. Bytes spilled to remote storage mean memory
pressure, so I'd fix the join or scale up. Partitions scanned close to total means no pruning, so I'd look
for functions on filter columns or a missing clustering key. High queued time is concurrency, so I'd use
multi-cluster rather than a bigger size. An exploding join means fan-out from a bad key. I'd also check the
result cache and whether a mart should pre-aggregate.

**How do you control cost?** One warehouse per workload with 60s auto-suspend, resource monitors per
warehouse, transient schemas for rebuildable layers, incremental models, serverless tasks that only run
when the stream has data, and a cost-per-dbt-model query on `query_tag`.

**How is PII protected?** Masking policies on email and phone, unmasked only for `PII_READER_ROLE`, plus a
row access policy by state and access-history auditing. Because dbt recreates tables, the policies are
re-attached in a dbt post-hook. That's a common gotcha.

**Exactly-once from the stream?** A stream's offset advances only when the consuming DML commits. The root
task copies the stream into a batch table in one transaction, and child tasks read the batch. A failed run
leaves the offset where it was.

**How do you know no data was lost?** Reconciliation at every hop: files landed vs loaded, rows per file vs
rows loaded, Lambda valid counts vs raw rows, API manifest counts vs raw, live CDC keys vs fact rows, and
rupee totals between staging and marts. Any mismatch fails the pipeline.

**Why DuckDB locally?** Snowflake costs money and needs an account. The dbt models use dispatched macros
(`json_str`, `flatten_json_array`, `incremental_strategy`), so the same SQL compiles to native Snowflake
syntax (VARIANT paths, LATERAL FLATTEN, MERGE) in prod and DuckDB in CI. Every PR runs the full pipeline.

## Be honest about
* The data is synthetic (the generator is in the repo, with injected issues).
* Locally verified: the Python components, dbt models and tests, DQ and reconciliation.
  Written but not deployed here: Snowflake DDL, Terraform, Step Functions, Glue.
  If you deploy them on a Snowflake trial plus the AWS free tier, say so, with screenshots.
