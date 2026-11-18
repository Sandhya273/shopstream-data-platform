"""
AWS Glue (PySpark) job: compact AWS DMS CDC CSV files into partitioned Parquet.

Why: DMS writes many small CSV files per table. Compacting them into ~128-256 MB
Parquet files (a) cuts Snowflake COPY overhead (fewer files, columnar, typed) and
(b) gives the data lake a query-friendly copy for Athena.

Job parameters:
  --JOB_NAME        glue job name
  --SOURCE_PATH     s3://<raw>/dms/<table>/
  --TARGET_PATH     s3://<raw>/dms_parquet/<table>/
  --TABLE_NAME      orders | order_items | customers | products

Uses Glue job bookmarks so each run only processes newly-arrived DMS files.
Note: this job targets the Glue 4.0 runtime; it is not executed by the local runner
(local runs load the CSVs directly, which is the same data path Snowflake uses for
small tables).
"""
import sys

from awsglue.context import GlueContext  # type: ignore
from awsglue.job import Job  # type: ignore
from awsglue.utils import getResolvedOptions  # type: ignore
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME", "SOURCE_PATH", "TARGET_PATH", "TABLE_NAME"])
sc = SparkContext()
glue = GlueContext(sc)
spark = glue.spark_session
job = Job(glue)
job.init(args["JOB_NAME"], args)

dyf = glue.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [args["SOURCE_PATH"]], "recurse": True},
    format="csv",
    format_options={"withHeader": True},
    transformation_ctx=f"src_{args['TABLE_NAME']}",  # enables job bookmarks
)
df = dyf.toDF()

if df.rdd.isEmpty():
    print("No new files - exiting")
    job.commit()
    sys.exit(0)

df = (
    df.withColumn("cdc_ts", F.to_timestamp("cdc_ts"))
      .withColumn("_source_file", F.input_file_name())
      .withColumn("_glue_run_ts", F.current_timestamp())
      .withColumn("cdc_date", F.to_date("cdc_ts"))
      .dropDuplicates()  # DMS task restarts can replay identical records
)

target_files = max(1, int(df.count() / 1_000_000))  # ~1M rows per output file
(
    df.repartition(target_files, "cdc_date")
      .write.mode("append")
      .partitionBy("cdc_date")
      .parquet(args["TARGET_PATH"])
)

job.commit()
