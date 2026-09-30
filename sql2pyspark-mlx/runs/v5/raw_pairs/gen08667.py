from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("latency_ms", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("latency_ms", "id", "key")
w3 = Window.partitionBy("level").orderBy(F.col("latency_ms").desc(), "id", "key")
w_running = Window.partitionBy("host").orderBy("latency_ms_bucket", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("latency_ms_bucket", "id", "key")
w6 = Window.partitionBy("level", "key").orderBy("row_seq", "id", "key")
w7 = Window.partitionBy("key")
w8 = Window.partitionBy("key").orderBy("log_ts", "id", "key")
w9 = Window.partitionBy("key").orderBy("log_ts", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("key").orderBy(F.col("prev_running_count_latency_ms_bucket").desc_nulls_first(), "id", "key")
w11 = Window.partitionBy("key").orderBy(F.col("prev_running_count_latency_ms_bucket").asc_nulls_first())
w12 = Window.partitionBy("key").orderBy("prev_row_seq", "id", "key")

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        F.explode("context").alias("key", "attr_value"),
    )
)

with_windows = (
    lines
    .withColumn("last_known_key", F.last("key", ignorenulls=True).over(w))
    .withColumn("latency_ms_bucket", F.ntile(4).over(w2))
    .withColumn("row_seq", F.row_number().over(w3))
)

with_windows_app_logs = (
    with_windows
    .select(
        "id",
        "log_ts",
        "level",
        "key",
        "attr_value",
        "last_known_key",
        "latency_ms_bucket",
        "row_seq",
        F.count("latency_ms_bucket").over(w_running).alias("running_count_latency_ms_bucket"),
        F.max("latency_ms_bucket").over(w_running).alias("running_max_latency_ms_bucket"),
        F.min("latency_ms_bucket").over(w_running).alias("running_min_latency_ms_bucket"),
    )
)

enriched = (
    with_windows_app_logs
    .select(
        "id",
        "log_ts",
        "key",
        "latency_ms_bucket",
        "running_count_latency_ms_bucket",
        F.lag("running_count_latency_ms_bucket", 2, 0).over(w5).alias("prev_running_count_latency_ms_bucket"),
        F.lag("row_seq", 1).over(w6).alias("prev_row_seq"),
        F.round((F.col("running_min_latency_ms_bucket") / F.sum("running_min_latency_ms_bucket").over(w7)), 4).alias("running_min_latency_ms_bucket_share"),
    )
)

gaps = (
    enriched
    .withColumn("is_new_session", F.when(F.lag("log_ts").over(w8).isNull() | (F.col("log_ts") > F.lag("log_ts").over(w8) + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

islands = gaps.withColumn("burst_id", F.sum("is_new_session").over(w9))

ranked = (
    islands
    .withColumn("latest_rank", F.row_number().over(w10))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

with_windows_app_logs_2 = (
    ranked
    .select(
        "id",
        "log_ts",
        "key",
        "prev_row_seq",
        "running_min_latency_ms_bucket_share",
        F.dense_rank().over(w11).alias("prev_running_count_latency_ms_bucket_dense_rank"),
        F.lag("prev_row_seq", 2).over(w12).alias("prev_prev_row_seq"),
    )
)

result = (
    with_windows_app_logs_2
    .select(
        "id",
        "key",
        "prev_row_seq",
        "running_min_latency_ms_bucket_share",
        "prev_running_count_latency_ms_bucket_dense_rank",
        "prev_prev_row_seq",
        F.when(F.col("key").rlike("^(region|trace)$"), "GROUP_A").when(F.col("key").rlike("^(version)$"), "GROUP_B").otherwise("CORE").alias("key_zone"),
    )
)
