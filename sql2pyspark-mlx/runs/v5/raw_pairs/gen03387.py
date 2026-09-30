from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy(F.col("latency_ms").desc_nulls_first())
w2 = Window.partitionBy("attr_key")
w3 = Window.partitionBy("host")
w4 = Window.partitionBy("attr_key", "host")
w5 = Window.partitionBy("attr_key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w6 = Window.orderBy(F.col("log_ts_period").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w7 = Window.partitionBy("attr_key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w_recent = Window.partitionBy("host", "attr_key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

base = (
    flattened
    .filter(F.col("log_ts") >= F.lit("2025-06-01").cast("date"))
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "attr_key",
        "attr_value",
        F.date_trunc("YEAR", "log_ts").alias("log_ts_period"),
    )
)

with_windows = (
    base
    .filter(F.col("log_ts_period") >= "2025-06-28")
    .select(
        "host",
        "log_ts",
        "latency_ms",
        "attr_key",
        "attr_value",
        "log_ts_period",
        F.percent_rank().over(w).alias("latency_ms_percent_rank"),
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w2)), 4).alias("latency_ms_share"),
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w3)), 4).alias("latency_ms_share_2"),
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w2)), 4).alias("latency_ms_share_3"),
        F.avg("latency_ms").over(w4).alias("attr_key_avg_latency_ms"),
    )
)

with_windows_app_logs = (
    with_windows
    .select(
        "host",
        "log_ts",
        "attr_key",
        "attr_value",
        "log_ts_period",
        "latency_ms_percent_rank",
        "latency_ms_share_2",
        F.count("attr_key_avg_latency_ms").over(w2).alias("attr_key_count_attr_key_avg_latency_ms"),
        F.count("latency_ms_share_3").over(w5).alias("latency_ms_share_3_count_2h"),
    )
)

result = (
    with_windows_app_logs
    .select(
        "host",
        "log_ts",
        "attr_value",
        "log_ts_period",
        "latency_ms_percent_rank",
        "latency_ms_share_2",
        "attr_key_count_attr_key_avg_latency_ms",
        F.avg("latency_ms_share_3_count_2h").over(w6).alias("latency_ms_share_3_count_2h_avg_30d"),
        F.avg("latency_ms_share_3_count_2h").over(w7).alias("latency_ms_share_3_count_2h_avg_1h"),
        F.max("latency_ms_share_2").over(w_recent).alias("latency_ms_share_2_max_30m"),
    )
)
