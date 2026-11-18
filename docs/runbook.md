# Operations runbook

| Symptom | Where to look | Likely cause → fix |
|---|---|---|
| SNS "pipeline FAILED" | Step Functions execution → failed state's `$.error` | see the state rows below |
| `ExtractPayments` failed | CloudWatch `/aws/lambda/shopstream-*-payments-extractor` | 401 → rotate the API key in Secrets Manager; repeated 429 → lower the page size. Watermark not advanced, so re-run is safe |
| `GlueCompact` failed | Glue job run logs | schema drift in DMS CSV → update the job; bookmarks make re-runs pick up only new files |
| `LoadSnowflakeRaw` failed | `COPY_HISTORY`, `snowflake/playbooks/loading_operations.sql` | parse error → `VALIDATION_MODE='RETURN_ERRORS'`, fix the file, reload with `FORCE=TRUE` for that file only |
| `DbtBuild` failed | ECS task logs (`/ecs/shopstream-*-dbt`) | test failure → read `target/run_results.json`; model error → `dbt build -s <model>+` after the fix |
| DQ critical failure | `ANALYTICS_DB.AUDIT.DQ_RESULTS` latest run | data issue → fix upstream, or downgrade the rule to warning with a ticket |
| Reconciliation mismatch | `AUDIT.RECONCILIATION` | files not loaded → check the Snowpipe status / SQS notification; row drift → compare `_source_file` counts |
| Clickstream stale | `SYSTEM$PIPE_STATUS('pipe_clickstream')`, task history | pipe paused / S3 event missing → `ALTER PIPE ... REFRESH`; task suspended after 3 failures → fix, `ALTER TASK ... RESUME` |
| Validator Lambda errors alarm | Lambda logs, DLQ `shopstream-*-ingestion-dlq` | redrive DLQ messages after the fix |
| Bad deploy changed a mart | Time Travel | `CREATE TABLE x_restore CLONE x AT(OFFSET => -3600); ALTER TABLE x SWAP WITH x_restore;` |
| Credits spike | `WAREHOUSE_METERING_HISTORY`, resource monitor emails | runaway query → `SYSTEM$CANCEL_QUERY`; check auto-suspend and cluster counts |

**Backfill:** start the state machine with `{"full_refresh": true, "dms_tables": [...]}`. Then run
`dbt build --full-refresh -s fct_orders+ fct_sessions+` and re-run `snowflake/post_dbt/` to re-attach the DMFs.
