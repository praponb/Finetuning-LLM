from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key").orderBy(F.col("latency_ms").asc_nulls_last())
w2 = Window.partitionBy("key").orderBy("latency_ms", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("latency_ms_cume_dist", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("key").orderBy("status_code")
w5 = Window.orderBy("status_code", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("host").orderBy(F.col("running_latency_ms").desc(), "id", "key")
w_hist = Window.partitionBy("host").orderBy("first_known_latency_ms_cume_dist", "id", "key")
w_recent = Window.partitionBy("key").orderBy("running_latency_ms_bucket", "id", "key")
w_grp = Window.partitionBy("key").orderBy("seq_no", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("host").orderBy("log_ts", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "attr_value"),
    )
)

cleaned = (
    lines
    .filter((F.col("log_ts") >= F.lit("2025-06-01").cast("date")) & F.col("key").isNotNull())
    .select(
        "*",
        F.when(F.col("level").isin(["DEBUG", "ERROR"]), "Tier 1").otherwise("Other").alias("level_group"),
    )
)

metrics = (
    cleaned
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "status_code",
        "key",
        "attr_value",
        "level_group",
        F.cume_dist().over(w).alias("latency_ms_cume_dist"),
        F.sum("latency_ms").over(w2).alias("running_latency_ms"),
    )
)

metrics_app_logs = (
    metrics
    .filter(F.col("latency_ms_cume_dist") >= 0.59)
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "status_code",
        "key",
        "attr_value",
        "running_latency_ms",
        F.sum("running_latency_ms").over(w3).alias("running_running_latency_ms"),
        F.percent_rank().over(w4).alias("status_code_percent_rank"),
        F.first("latency_ms_cume_dist", ignorenulls=True).over(w5).alias("first_known_latency_ms_cume_dist"),
        F.ntile(10).over(w6).alias("running_latency_ms_bucket"),
    )
)

with_windows = (
    metrics_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "status_code",
        "key",
        "attr_value",
        "running_latency_ms",
        "running_running_latency_ms",
        "status_code_percent_rank",
        "first_known_latency_ms_cume_dist",
        F.row_number().over(w_hist).alias("seq_no"),
        F.lag("status_code_percent_rank", 1).over(w_recent).alias("prev_status_code_percent_rank"),
    )
)

with_windows_app_logs = (
    with_windows
    .select(
        "id",
        "status_code",
        "key",
        "attr_value",
        "running_latency_ms",
        "first_known_latency_ms_cume_dist",
        "seq_no",
        F.sum("running_latency_ms").over(w_grp).alias("cumulative_running_latency_ms"),
        F.last("seq_no", ignorenulls=True).over(w10).alias("last_known_seq_no"),
    )
)

valid = (
    with_windows_app_logs
    .filter(F.col("key").isNotNull())
    .select(
        "id",
        "status_code",
        "key",
        "running_latency_ms",
        "seq_no",
        "cumulative_running_latency_ms",
        F.col("running_latency_ms").cast("string").alias("running_latency_ms_num"),
    )
)

result = (
    valid
    .select(
        "id",
        "status_code",
        "key",
        "cumulative_running_latency_ms",
        F.when(F.col("key").rlike("^(version|trace)$"), "OTHER_ZONE").otherwise("CORE").alias("key_zone"),
    )
    .orderBy(F.col("status_code").desc())
)
