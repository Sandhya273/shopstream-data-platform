-- =====================================================================
-- V1.0.6  Near-real-time processing with Streams + Tasks (task graph)
--
--  pipe_clickstream ──► CLICKSTREAM_EVENTS ──(stream)──► t_clickstream_root (every 5 min, only if data)
--                                                           ├─► t_flatten_purchase_items  (LATERAL FLATTEN)
--                                                           └─► t_refresh_sessions        (MERGE into fct_sessions_rt)
--                                                                     └─► t_load_audit    (finalizer)
-- Streams give exactly-once change consumption: offsets only advance when the
-- consuming DML transaction commits.
-- =====================================================================
use role transformer_role;
use warehouse transform_wh;

create stream if not exists raw_db.raw.clickstream_events_stream
  on table raw_db.raw.clickstream_events append_only = true;

create table if not exists analytics_db.marts.purchase_items_rt (
  event_id varchar, order_id number, product_id number, quantity number,
  event_ts timestamp_ntz, _processed_at timestamp_ltz
);

create table if not exists analytics_db.marts.fct_sessions_rt (
  session_id varchar, customer_id number, session_start_ts timestamp_ntz, session_end_ts timestamp_ntz,
  events number, reached_add_to_cart boolean, converted boolean, purchase_revenue number(12,2),
  device_type varchar, utm_source varchar, _updated_at timestamp_ltz
) cluster by (to_date(session_start_ts));

-- staging table for one micro-batch (the stream is read ONCE per run, by the root task)
create transient table if not exists analytics_db.intermediate.clickstream_batch like raw_db.raw.clickstream_events;

-- Root: serverless task (Snowflake sizes compute), skipped when the stream is empty
create or replace task analytics_db.intermediate.t_clickstream_root
  schedule = '5 MINUTE'
  user_task_managed_initial_warehouse_size = 'XSMALL'
  suspend_task_after_num_failures = 3
  when system$stream_has_data('RAW_DB.RAW.CLICKSTREAM_EVENTS_STREAM')
as
begin
  truncate table analytics_db.intermediate.clickstream_batch;
  insert into analytics_db.intermediate.clickstream_batch (raw, _source_file, _file_row_number, _loaded_at)
    select raw, _source_file, _file_row_number, _loaded_at from raw_db.raw.clickstream_events_stream;
end;

-- Child 1: explode nested arrays
create or replace task analytics_db.intermediate.t_flatten_purchase_items
  warehouse = transform_wh
  after analytics_db.intermediate.t_clickstream_root
as
insert into analytics_db.marts.purchase_items_rt
select
  b.raw:event_id::varchar,
  b.raw:properties.order_id::number,
  it.value:product_id::number,
  it.value:qty::number,
  try_to_timestamp_ntz(b.raw:event_ts::varchar),
  current_timestamp()
from analytics_db.intermediate.clickstream_batch b,
     lateral flatten(input => b.raw:properties.items) it
where b.raw:event_type::varchar = 'purchase';

-- Child 2: upsert session aggregates (late events re-aggregate the whole session)
create or replace task analytics_db.intermediate.t_refresh_sessions
  warehouse = transform_wh
  after analytics_db.intermediate.t_clickstream_root
as
merge into analytics_db.marts.fct_sessions_rt t
using (
  with touched as (select distinct raw:session_id::varchar as session_id from analytics_db.intermediate.clickstream_batch),
  ev as (
    select e.raw, try_to_timestamp_ntz(e.raw:event_ts::varchar) as ts
    from raw_db.raw.clickstream_events e
    join touched s on s.session_id = e.raw:session_id::varchar
    qualify row_number() over (partition by e.raw:event_id::varchar order by e._loaded_at) = 1
  )
  select
    raw:session_id::varchar                                         as session_id,
    max(raw:customer_id::number)                                    as customer_id,
    min(ts) as session_start_ts, max(ts) as session_end_ts, count(*) as events,
    boolor_agg(raw:event_type::varchar = 'add_to_cart')             as reached_add_to_cart,
    boolor_agg(raw:event_type::varchar = 'purchase')                as converted,
    coalesce(sum(raw:properties.revenue::number(12,2)), 0)          as purchase_revenue,
    min_by(raw:device.type::varchar, ts)                            as device_type,
    min_by(raw:utm.source::varchar, ts)                             as utm_source
  from ev group by 1
) s
on t.session_id = s.session_id
when matched then update set
  customer_id = s.customer_id, session_start_ts = s.session_start_ts, session_end_ts = s.session_end_ts,
  events = s.events, reached_add_to_cart = s.reached_add_to_cart, converted = s.converted,
  purchase_revenue = s.purchase_revenue, _updated_at = current_timestamp()
when not matched then insert values
  (s.session_id, s.customer_id, s.session_start_ts, s.session_end_ts, s.events, s.reached_add_to_cart,
   s.converted, s.purchase_revenue, s.device_type, s.utm_source, current_timestamp());

-- Finalizer: always runs after the graph (success or failure) -> audit row
create or replace task analytics_db.intermediate.t_load_audit
  warehouse = load_wh
  finalize = 'ANALYTICS_DB.INTERMEDIATE.T_CLICKSTREAM_ROOT'
as
insert into raw_db.raw.load_history
select file_name, table_name, row_count, error_count, status, first_error_message, last_load_time
from table(raw_db.information_schema.copy_history(
       table_name => 'RAW_DB.RAW.CLICKSTREAM_EVENTS',
       start_time => dateadd(minute, -10, current_timestamp())));

-- children first, root last
alter task analytics_db.intermediate.t_load_audit resume;
alter task analytics_db.intermediate.t_refresh_sessions resume;
alter task analytics_db.intermediate.t_flatten_purchase_items resume;
alter task analytics_db.intermediate.t_clickstream_root resume;

-- ---------------------------------------------------------------------
-- Alternative for simple cases: a Dynamic Table declares the result and
-- Snowflake keeps it within TARGET_LAG (no stream/task plumbing).
-- ---------------------------------------------------------------------
create or replace dynamic table analytics_db.marts.dt_hourly_traffic
  target_lag = '15 minutes'
  warehouse = transform_wh
as
select date_trunc('hour', try_to_timestamp_ntz(raw:event_ts::varchar)) as event_hour,
       raw:device.type::varchar as device_type,
       count(*) as events,
       count(distinct raw:session_id::varchar) as sessions,
       count_if(raw:event_type::varchar = 'purchase') as purchases
from raw_db.raw.clickstream_events
group by 1, 2;
