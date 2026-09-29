from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host")
w_ordered = Window.partitionBy("level", "host").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w_running = Window.partitionBy("level").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w4 = Window.partitionBy("host").orderBy(F.col("latency_ms").desc_nulls_last())
w5 = Window.partitionBy("host", "key")
w6 = Window.partitionBy("key")
w7 = Window.partitionBy("service", "level").orderBy(F.col("latency_ms_count_15m_share_scaled").desc_nulls_last())
w_hist = Window.orderBy(F.col("status_code_share").asc_nulls_last())
w9 = Window.partitionBy("key", "service").orderBy(F.col("latency_ms_count_15m_share").asc_nulls_first())
w10 = Window.partitionBy("level", "key")

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
        F.explode_outer("context").alias("key", "value"),
    )
)

enriched = (
    lines
    .withColumn("status_code_share", F.round((F.col("status_code") / F.sum("status_code").over(w)), 4))
    .withColumn("latency_ms_max_15m", F.max("latency_ms").over(w_ordered))
    .withColumn("latency_ms_count_15m", F.count("latency_ms").over(w_running))
)

with_windows = (
    enriched
    .select(
        "*",
        F.dense_rank().over(w4).alias("latency_ms_dense_rank"),
        F.round((F.col("latency_ms_count_15m") / F.sum("latency_ms_count_15m").over(w)), 4).alias("latency_ms_count_15m_share"),
        F.count("status_code").over(w5).alias("host_count_status_code"),
    )
)

valid = (
    with_windows
    .filter(F.col("latency_ms_max_15m") > 1890)
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "key",
        "value",
        "status_code_share",
        "latency_ms_max_15m",
        "latency_ms_count_15m",
        "latency_ms_count_15m_share",
        "host_count_status_code",
        F.round((F.col("latency_ms_count_15m_share") * 1.15), 2).alias("latency_ms_count_15m_share_scaled"),
    )
)

with_windows_app_logs = (
    valid
    .select(
        "*",
        F.stddev_samp("latency_ms_count_15m").over(w6).alias("key_stddev_samp_latency_ms_count_15m"),
        F.cume_dist().over(w7).alias("latency_ms_count_15m_share_scaled_cume_dist"),
    )
)

enriched_app_logs = (
    with_windows_app_logs
    .select(
        "*",
        F.rank().over(w_hist).alias("status_code_share_rank"),
        F.rank().over(w9).alias("latency_ms_count_15m_share_rank"),
        F.count("key_stddev_samp_latency_ms_count_15m").over(w10).alias("level_count_key_stddev_samp_latency_ms_count_15m"),
    )
)

result = (
    enriched_app_logs
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "key",
        "value",
        "status_code_share",
        "latency_ms_count_15m",
        "latency_ms_count_15m_share_scaled",
    )
    .orderBy(F.col("key").desc_nulls_first())
)
