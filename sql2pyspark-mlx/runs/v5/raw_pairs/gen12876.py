from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w2 = Window.partitionBy("key")
w3 = Window.partitionBy("level")
w4 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w5 = Window.partitionBy("key").orderBy(F.col("latency_ms_sum_15m_times_level_min_latency_ms").desc_nulls_last())
w_grp = Window.partitionBy("key").orderBy(F.col("latency_ms_sum_15m_times_level_min_latency_ms").desc())
w7 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w8 = Window.partitionBy("level_min_latency_ms_tier").orderBy(F.col("num_keys").desc())

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

scored = (
    lines
    .select(
        "log_ts",
        "key",
        "attr_value",
        F.avg("latency_ms").over(w).alias("latency_ms_avg_2h"),
        F.stddev_samp("latency_ms").over(w2).alias("key_stddev_samp_latency_ms"),
        F.min("latency_ms").over(w3).alias("level_min_latency_ms"),
        F.sum("latency_ms").over(w4).alias("latency_ms_sum_15m"),
    )
)

cleaned = (
    scored
    .filter(F.col("key") != "region")
    .select(
        "*",
        (F.col("latency_ms_sum_15m") * F.col("level_min_latency_ms")).alias("latency_ms_sum_15m_times_level_min_latency_ms"),
        F.coalesce("key", F.lit("UNKNOWN")).alias("key_clean"),
    )
)

enriched = (
    cleaned
    .select(
        "*",
        F.stddev_samp("latency_ms_sum_15m_times_level_min_latency_ms").over(w2).alias("key_stddev_samp_latency_ms_sum_15m_times_level_min_latency_ms"),
        F.percent_rank().over(w5).alias("latency_ms_sum_15m_times_level_min_latency_ms_percent_rank"),
        F.avg("key_stddev_samp_latency_ms").over(w2).alias("key_avg_key_stddev_samp_latency_ms"),
        F.cume_dist().over(w_grp).alias("latency_ms_sum_15m_times_level_min_latency_ms_cume_dist"),
        F.count("latency_ms_avg_2h").over(w7).alias("latency_ms_avg_2h_count_1h"),
    )
)

valid = (
    enriched
    .filter(F.col("key").isNotNull() & (~F.col("key").eqNullSafe(F.lit("region"))))
    .withColumn("latency_ms_sum_15m_times_level_min_latency_ms_cume_dist_per_key_stddev_samp_latency_ms", F.round((F.col("latency_ms_sum_15m_times_level_min_latency_ms_cume_dist") / F.col("key_stddev_samp_latency_ms")), 2))
    .withColumn("log_ts_date", F.to_date("log_ts"))
    .withColumn("key_filled", F.coalesce("key", F.lit("OTHER")))
)

result = (
    valid
    .groupBy(
        F.when(F.col("level_min_latency_ms") >= 1875, "PREMIUM").otherwise("STANDARD").alias("level_min_latency_ms_tier"),
        F.when(F.col("latency_ms_sum_15m_times_level_min_latency_ms_cume_dist") >= 0.63, "LARGE").otherwise("SMALL").alias("latency_ms_sum_15m_times_level_min_latency_ms_cume_dist_tier"),
    )
    .agg(
        F.size(F.collect_set("key")).alias("num_keys"),
        F.count(F.when(F.col("key") == "trace", 1)).alias("trace_rows"),
        F.max("log_ts").alias("last_log_ts"),
        F.percentile_approx("key_stddev_samp_latency_ms_sum_15m_times_level_min_latency_ms", [0.25, 0.5, 0.75]).alias("key_stddev_samp_latency_ms_sum_15m_times_level_min_latency_ms_quartiles"),
        F.min("log_ts").alias("first_seen"),
        F.count(F.when(F.col("key") == "trace", 1)).alias("trace_count"),
    )
    .withColumn("num_keys_percent_rank", F.percent_rank().over(w8))
    .orderBy(F.col("level_min_latency_ms_tier").asc_nulls_last())
)
