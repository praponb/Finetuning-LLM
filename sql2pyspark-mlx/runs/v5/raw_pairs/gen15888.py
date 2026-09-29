from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level")
w2 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w3 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w4 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w5 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w6 = Window.orderBy(F.col("level_avg_latency_ms_share_max_2h").desc_nulls_last())
w_grp = Window.orderBy(F.col("level_min_latency_ms_share_avg_30m").asc_nulls_last())

exploded = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("key", "value"),
    )
)

scored = (
    exploded
    .filter(F.hour("log_ts").between(8, 12))
    .select(
        "log_ts",
        "level",
        "latency_ms",
        "value",
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w)), 4).alias("latency_ms_share"),
        F.avg("latency_ms").over(w2).alias("latency_ms_avg_30m"),
    )
)

filtered = (
    scored
    .filter(F.col("latency_ms").between(1247, 1352))
    .select("*", F.col("log_ts").cast("date").alias("log_ts_day"))
)

scored_app_logs = (
    filtered
    .select(
        "log_ts",
        "value",
        "latency_ms_avg_30m",
        F.min("latency_ms_share").over(w).alias("level_min_latency_ms_share"),
        F.stddev_samp("latency_ms").over(w).alias("level_stddev_samp_latency_ms"),
        F.avg("latency_ms_share").over(w).alias("level_avg_latency_ms_share"),
    )
)

enriched = (
    scored_app_logs
    .select(
        "*",
        F.avg("level_min_latency_ms_share").over(w2).alias("level_min_latency_ms_share_avg_30m"),
        F.sum("level_avg_latency_ms_share").over(w3).alias("level_avg_latency_ms_share_sum_15m"),
        F.max("level_avg_latency_ms_share").over(w4).alias("level_avg_latency_ms_share_max_2h"),
    )
)

valid = (
    enriched
    .filter((F.col("log_ts") >= F.lit("2025-06-01 06:00:00").cast("timestamp")) & (F.to_date("log_ts") >= "2025-03-30"))
    .select(
        "log_ts",
        "value",
        "level_stddev_samp_latency_ms",
        "level_min_latency_ms_share_avg_30m",
        "level_avg_latency_ms_share_max_2h",
        (F.col("level_min_latency_ms_share_avg_30m") / F.nullif("level_min_latency_ms_share", F.lit(0))).alias("level_min_latency_ms_share_avg_30m_per_level_min_latency_ms_share"),
    )
)

scored_app_logs_2 = (
    valid
    .select(
        "*",
        F.max("level_stddev_samp_latency_ms").over(w5).alias("level_stddev_samp_latency_ms_max_1h"),
        F.max("level_min_latency_ms_share_avg_30m").over(w4).alias("level_min_latency_ms_share_avg_30m_max_2h"),
        F.percent_rank().over(w6).alias("level_avg_latency_ms_share_max_2h_percent_rank"),
        F.cume_dist().over(w_grp).alias("level_min_latency_ms_share_avg_30m_cume_dist"),
    )
)

result = (
    scored_app_logs_2
    .select(
        "log_ts",
        "level_stddev_samp_latency_ms",
        "level_avg_latency_ms_share_max_2h",
        "level_min_latency_ms_share_avg_30m_per_level_min_latency_ms_share",
        "level_min_latency_ms_share_avg_30m_max_2h",
        "level_avg_latency_ms_share_max_2h_percent_rank",
        "level_min_latency_ms_share_avg_30m_cume_dist",
    )
    .orderBy(F.col("log_ts").desc_nulls_last(), "level_avg_latency_ms_share_max_2h")
)
