from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("latency_ms").desc_nulls_last())
w2 = Window.partitionBy("service", "level")
w3 = Window.partitionBy("level")
w4 = Window.partitionBy("service")
w5 = Window.partitionBy("service", "k")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

metrics = (
    lines
    .withColumn("latency_ms_dense_rank", F.dense_rank().over(w))
    .withColumn("service_avg_latency_ms", F.avg("latency_ms").over(w2))
    .withColumn("level_sum_latency_ms", F.sum("latency_ms").over(w3))
)

with_windows = (
    metrics
    .select(
        "*",
        F.round((F.col("level_sum_latency_ms") / F.sum("level_sum_latency_ms").over(w3)), 4).alias("level_sum_latency_ms_share"),
        F.count("service_avg_latency_ms").over(w4).alias("service_count_service_avg_latency_ms"),
        F.count("level_sum_latency_ms").over(w5).alias("service_count_level_sum_latency_ms"),
    )
)

kept_keys = (
    with_windows
    .select("id", "host")
    .intersectAll(
        df_app_logs
        .filter(F.col("latency_ms").between(1870, 2158))
        .select("id", "host")
    )
)

verified = (
    with_windows
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.host") == F.col("k.host")), "inner")
    .select("s.*")
)

summary = (
    verified
    .groupBy(F.date_trunc("HOUR", "log_ts").alias("hour_window"))
    .agg(
        F.round(F.avg("level_sum_latency_ms_share"), 2).alias("avg_level_sum_latency_ms_share"),
        F.max("log_ts").alias("ended_at"),
        F.percentile_approx("service_avg_latency_ms", [0.25, 0.5, 0.75]).alias("service_avg_latency_ms_quartiles"),
        F.round(F.sum("level_sum_latency_ms") / F.nullif(F.count(F.when(F.col("k") == "version", 1)), F.lit(0)), 2).alias("level_sum_latency_ms_per_version"),
    )
)

result = (
    summary
    .select(
        "hour_window",
        "service_avg_latency_ms_quartiles",
        "level_sum_latency_ms_per_version",
    )
)
