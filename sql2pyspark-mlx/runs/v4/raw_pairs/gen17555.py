from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "service")
w_grp = Window.partitionBy("level")
w3 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w4 = Window.partitionBy("service", "level").orderBy(F.col("latency_ms").desc_nulls_first())
w5 = Window.orderBy(F.col("latency_ms_sum_1h").desc())
w6 = Window.partitionBy("service", "key")
w7 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w8 = Window.partitionBy("service", "level").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w9 = Window.partitionBy("service", "level")
w10 = Window.partitionBy("level").orderBy(F.col("level_max_latency_ms_share").desc_nulls_last())
w11 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w12 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w13 = Window.orderBy(F.col("service_max_level_max_latency_ms").desc_nulls_last())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("key", "v"),
    )
)

enriched = (
    exploded
    .withColumn("latency_ms_share", F.round((F.col("latency_ms") / F.sum("latency_ms").over(w)), 4))
    .withColumn("level_max_latency_ms", F.max("latency_ms").over(w_grp))
    .withColumn("level_stddev_samp_latency_ms", F.stddev_samp("latency_ms").over(w_grp))
    .withColumn("latency_ms_sum_1h", F.sum("latency_ms").over(w3))
    .withColumn("latency_ms_rank", F.rank().over(w4))
)

with_windows = (
    enriched
    .withColumn("latency_ms_sum_1h_percent_rank", F.percent_rank().over(w5))
    .withColumn("service_max_level_max_latency_ms", F.max("level_max_latency_ms").over(w6))
    .withColumn("latency_ms_share_sum_30m", F.sum("latency_ms_share").over(w7))
)

with_windows_app_logs = (
    with_windows
    .select(
        "*",
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w_grp)), 4).alias("latency_ms_share_2"),
        F.sum("level_stddev_samp_latency_ms").over(w8).alias("level_stddev_samp_latency_ms_sum_15m"),
        F.round((F.col("level_max_latency_ms") / F.sum("level_max_latency_ms").over(w9)), 4).alias("level_max_latency_ms_share"),
    )
)

valid = (
    with_windows_app_logs
    .filter((F.col("log_ts") > F.lit("2025-06-01").cast("date")) & F.col("key").isNotNull() & (F.col("latency_ms") == 1022))
    .select(
        "*",
        (F.col("latency_ms_sum_1h") * F.col("latency_ms_sum_1h_percent_rank")).alias("latency_ms_sum_1h_times_latency_ms_sum_1h_percent_rank"),
    )
)

enriched_app_logs = (
    valid
    .select(
        "*",
        F.dense_rank().over(w10).alias("level_max_latency_ms_share_dense_rank"),
        F.max("level_max_latency_ms").over(w11).alias("level_max_latency_ms_max_15m"),
        F.max("latency_ms_sum_1h_times_latency_ms_sum_1h_percent_rank").over(w12).alias("latency_ms_sum_1h_times_latency_ms_sum_1h_percent_rank_max_2h"),
        F.cume_dist().over(w13).alias("service_max_level_max_latency_ms_cume_dist"),
    )
)

daily = (
    enriched_app_logs
    .cube(
        F.date_trunc("HOUR", "log_ts").alias("hour_window"),
        F.when(F.col("level_max_latency_ms_share_dense_rank") >= 8, "LARGE").otherwise("SMALL").alias("level_max_latency_ms_share_dense_rank_tier"),
    )
    .agg(
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("duration_min"),
        F.percentile_approx("level_stddev_samp_latency_ms", [0.25, 0.5, 0.75]).alias("level_stddev_samp_latency_ms_quartiles"),
        F.count(F.when(F.col("key") == "region", 1)).alias("region_count"),
        F.bit_or("latency_ms").alias("latency_ms_bit_or"),
    )
)

result = (
    daily
    .select(
        "level_max_latency_ms_share_dense_rank_tier",
        "duration_min",
        "level_stddev_samp_latency_ms_quartiles",
        "region_count",
        "latency_ms_bit_or",
    )
    .orderBy("duration_min", "region_count")
)
