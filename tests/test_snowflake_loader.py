from ingestion.snowflake_loader.handler import build_copy_sql


def test_copy_sql_selects_all_columns_plus_metadata():
    sql = build_copy_sql("dms_orders", "@raw_db.raw.stg_dms/orders/", 11)
    assert "$11, metadata$filename, metadata$file_row_number" in sql
    assert "$12" not in sql
    assert "on_error = 'ABORT_STATEMENT'" in sql
