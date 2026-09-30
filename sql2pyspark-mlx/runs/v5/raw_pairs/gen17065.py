from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy("latency_ms", "id", "attr_key").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("level").orderBy("log_ts", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("level", "attr_key").orderBy("log_ts", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("level", "attr_key").orderBy(F.col("status_code").desc(), "id", "attr_key")
w5 = Window.partitionBy("attr_key").orderBy(F.col("status_code").desc(), "id", "attr_key")
w6 = Window.partitionBy("level").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w7 = Window.orderBy(F.col("row_seq").desc(), "id", "attr_key")
w8 = Window.partitionBy("level")
w9 = Window.partitionBy("level").orderBy("event_seq_sum_2h", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("level").orderBy("row_seq_2", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.partitionBy("service", "attr_key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w_running = Window.orderBy(F.col("row_seq_2").asc_nulls_first())
w13 = Window.orderBy(F.col("status_code").desc(), "id", "attr_key")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

with_windows = (
    lines
    .filter(F.col("level") != "DEBUG")
    .select(
        "*",
        F.avg("latency_ms").over(w).alias("latency_ms_avg_2"),
        F.last("attr_key", ignorenulls=True).over(w2).alias("last_known_attr_key"),
        F.last("latency_ms", ignorenulls=True).over(w3).alias("last_known_latency_ms"),
        F.row_number().over(w4).alias("row_seq"),
        F.row_number().over(w5).alias("event_seq"),
    )
)

enriched = (
    with_windows
    .withColumn("event_seq_sum_2h", F.sum("event_seq").over(w6))
    .withColumn("row_seq_2", F.row_number().over(w7))
    .withColumn("last_known_latency_ms_share", F.round((F.col("last_known_latency_ms") / F.sum("last_known_latency_ms").over(w8)), 4))
)

metrics = (
    enriched
    .select(
        "*",
        F.sum("last_known_latency_ms_share").over(w9).alias("running_sum_last_known_latency_ms_share"),
        F.count("last_known_latency_ms_share").over(w9).alias("running_count_last_known_latency_ms_share"),
        F.avg("last_known_latency_ms_share").over(w9).alias("running_avg_last_known_latency_ms_share"),
    )
)

with_windows_app_logs = (
    metrics
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "attr_key",
        "attr_value",
        "latency_ms_avg_2",
        "event_seq",
        "row_seq_2",
        "last_known_latency_ms_share",
        "running_sum_last_known_latency_ms_share",
        "running_avg_last_known_latency_ms_share",
        F.first("running_avg_last_known_latency_ms_share", ignorenulls=True).over(w10).alias("first_known_running_avg_last_known_latency_ms_share"),
        F.avg("last_known_latency_ms_share").over(w11).alias("last_known_latency_ms_share_avg_2h"),
        F.cume_dist().over(w_running).alias("row_seq_2_cume_dist"),
        F.row_number().over(w13).alias("seq_no"),
    )
)

result = (
    with_windows_app_logs
    .groupBy(
        F.when(F.col("running_sum_last_known_latency_ms_share") >= 3.1, "HIGH").otherwise("LOW").alias("running_sum_last_known_latency_ms_share_tier"),
    )
    .agg(
        F.sum(F.when(F.col("service") == "search", F.col("last_known_latency_ms_share")).otherwise(0)).alias("search_last_known_latency_ms_share"),
        F.percentile_approx("row_seq_2", 0.5).alias("row_seq_2_p50"),
        F.bit_or("latency_ms").alias("latency_ms_bit_or"),
        F.sum(F.when(F.col("level") == "DEBUG", F.col("last_known_latency_ms_share_avg_2h")).otherwise(0)).alias("debug_last_known_latency_ms_share_avg_2h"),
        F.count_if(F.col("level") == "DEBUG").alias("debug_events"),
    )
)
