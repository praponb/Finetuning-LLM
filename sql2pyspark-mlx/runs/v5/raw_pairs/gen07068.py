from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy(F.col("log_ts_hour").desc_nulls_first())
w2 = Window.partitionBy("attr_key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w_ordered = Window.partitionBy("attr_key")
w4 = Window.orderBy("log_ts_hour")
w5 = Window.partitionBy("attr_key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)

lines = (
    df_app_logs
    .select("id", "log_ts", F.explode_outer("context").alias("attr_key", "attr_value"))
)

filtered = (
    lines
    .filter((~F.col("attr_key").eqNullSafe(F.lit("trace"))) & F.col("attr_key").isin(["trace", "region", "version"]))
    .withColumn("log_ts_hour", F.hour("log_ts"))
    .withColumn("resolved_attr_key", F.coalesce("attr_key", F.lit("DIRECT")))
)

metrics = (
    filtered
    .select(
        "*",
        F.percent_rank().over(w_running).alias("log_ts_hour_percent_rank"),
        F.sum("log_ts_hour").over(w2).alias("log_ts_hour_sum_1h"),
        F.min("log_ts_hour").over(w_ordered).alias("attr_key_min_log_ts_hour"),
        F.cume_dist().over(w4).alias("log_ts_hour_cume_dist"),
        F.sum("log_ts_hour").over(w_ordered).alias("attr_key_sum_log_ts_hour"),
    )
)

cleaned = (
    metrics
    .filter((F.col("log_ts") >= F.to_date(F.lit("2025-06-01"))) & (F.col("log_ts_hour_sum_1h") >= 358))
    .select(
        "*",
        F.round("attr_key_sum_log_ts_hour", 1).alias("attr_key_sum_log_ts_hour_rounded"),
        F.floor("log_ts_hour_sum_1h").alias("log_ts_hour_sum_1h_whole"),
    )
)

with_windows = (
    cleaned
    .select(
        "log_ts",
        "attr_key",
        "attr_value",
        "log_ts_hour",
        "resolved_attr_key",
        "log_ts_hour_sum_1h",
        "attr_key_min_log_ts_hour",
        "attr_key_sum_log_ts_hour_rounded",
        "log_ts_hour_sum_1h_whole",
        F.count("log_ts_hour_percent_rank").over(w_ordered).alias("attr_key_count_log_ts_hour_percent_rank"),
        F.stddev_samp("log_ts_hour_sum_1h").over(w_ordered).alias("attr_key_stddev_samp_log_ts_hour_sum_1h"),
    )
)

with_windows_app_logs = (
    with_windows
    .select(
        "attr_key_sum_log_ts_hour_rounded",
        "log_ts_hour_sum_1h_whole",
        "attr_key_count_log_ts_hour_percent_rank",
        "attr_key_stddev_samp_log_ts_hour_sum_1h",
        F.stddev_samp("attr_key_min_log_ts_hour").over(w_ordered).alias("attr_key_stddev_samp_attr_key_min_log_ts_hour"),
        F.sum("attr_key_sum_log_ts_hour_rounded").over(w5).alias("attr_key_sum_log_ts_hour_rounded_sum_2h"),
    )
)

result = (
    with_windows_app_logs
    .select(
        "attr_key_count_log_ts_hour_percent_rank",
        "attr_key_stddev_samp_log_ts_hour_sum_1h",
        "attr_key_stddev_samp_attr_key_min_log_ts_hour",
        "attr_key_sum_log_ts_hour_rounded_sum_2h",
    )
)
