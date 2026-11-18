-- =====================================================================
-- V1.0.4  Bronze tables. Land everything as delivered + load metadata.
--     CDC tables are all VARCHAR on purpose: a bad value never fails a
--     load; typing happens in dbt staging with TRY_CAST.
-- =====================================================================
use role loader_role;
use schema raw_db.raw;

create table if not exists dms_orders (
  op varchar, cdc_ts varchar, order_id varchar, customer_id varchar, order_ts varchar,
  order_status varchar, channel varchar, shipping_city varchar, promo_code varchar,
  shipping_fee varchar, updated_at varchar,
  _source_file varchar, _file_row_number number, _loaded_at timestamp_ltz default current_timestamp()
) cluster by (to_date(try_to_timestamp(cdc_ts)));

create table if not exists dms_order_items (
  op varchar, cdc_ts varchar, order_item_id varchar, order_id varchar, product_id varchar,
  quantity varchar, unit_price varchar, discount_amount varchar, updated_at varchar,
  _source_file varchar, _file_row_number number, _loaded_at timestamp_ltz default current_timestamp()
);

create table if not exists dms_customers (
  op varchar, cdc_ts varchar, customer_id varchar, first_name varchar, last_name varchar,
  email varchar, phone varchar, city varchar, state varchar, country varchar, segment varchar,
  signup_ts varchar, updated_at varchar,
  _source_file varchar, _file_row_number number, _loaded_at timestamp_ltz default current_timestamp()
);

create table if not exists dms_products (
  op varchar, cdc_ts varchar, product_id varchar, sku varchar, product_name varchar,
  category varchar, sub_category varchar, brand varchar, unit_price varchar, unit_cost varchar,
  is_active varchar, updated_at varchar,
  _source_file varchar, _file_row_number number, _loaded_at timestamp_ltz default current_timestamp()
);

-- semi-structured sources keep the whole document in a VARIANT
create table if not exists payments (
  raw variant, _source_file varchar, _file_row_number number,
  _loaded_at timestamp_ltz default current_timestamp()
);

create table if not exists clickstream_events (
  raw variant, _source_file varchar, _file_row_number number,
  _loaded_at timestamp_ltz default current_timestamp()
);

-- unified load audit (populated from COPY_HISTORY by task in 06)
create table if not exists raw_db.raw.load_history (
  file_name varchar, target_table varchar, row_count number, error_count number,
  status varchar, first_error varchar, loaded_at timestamp_ltz
);
