from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("attr_key").orderBy("status_code", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("attr_key").orderBy("has_fault", "id", "attr_key")
w_running = Window.partitionBy("attr_key")
w4 = Window.partitionBy("attr_key").orderBy("status_code", "id", "attr_key")

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("attr_key", "v"),
    )
)

flattened_attrs = (
    lines
    .filter(F.col("status_code").between(323, 383))
    .select(
        "id",
        "status_code",
        "attr_key",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("has_fault"),
    )
)

metrics = (
    flattened_attrs
    .withColumn("first_known_status_code_high", F.first("status_code_high", ignorenulls=True).over(w_grp))
    .withColumn("event_seq", F.row_number().over(w_part))
    .withColumn("attr_key_sum_status_code_high", F.sum("status_code_high").over(w_running))
    .withColumn("prev_has_fault", F.lag("has_fault", 1).over(w_part))
    .withColumn("status_code_bucket", F.ntile(3).over(w4))
)

base = (
    metrics
    .filter(F.col("attr_key").isNotNull())
    .select(
        "id",
        "status_code",
        "attr_key",
        "has_fault",
        "first_known_status_code_high",
        "prev_has_fault",
        "status_code_bucket",
        F.col("status_code_bucket").cast("string").alias("status_code_bucket_num"),
    )
)

result = (
    base
    .groupBy("attr_key")
    .agg(
        F.count(F.when(F.col("attr_key") == "region", 1)).alias("region_rows"),
        F.percentile_approx("status_code", [0.25, 0.5, 0.75]).alias("status_code_quartiles"),
        F.min("status_code_bucket").alias("min_status_code_bucket"),
        F.sum(F.when(F.col("attr_key") == "region", F.col("has_fault")).otherwise(0)).alias("region_has_fault"),
        F.countDistinct("id").alias("distinct_ids"),
        F.size(F.collect_set("attr_key")).alias("num_attr_keys"),
    )
    .filter(F.col("distinct_ids") >= 1)
    .orderBy(F.col("num_attr_keys").desc())
)
