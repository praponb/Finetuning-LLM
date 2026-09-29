from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("level").orderBy("latency_ms")
w_recent_2 = Window.partitionBy("k")
w3 = Window.partitionBy("service", "level")
w4 = Window.partitionBy("service")
w5 = Window.partitionBy("k", "level").orderBy(F.col("latency_ms_percent_rank").desc())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

enriched = (
    exploded
    .withColumn("latency_ms_percent_rank", F.percent_rank().over(w_recent))
    .withColumn("k_stddev_samp_latency_ms", F.stddev_samp("latency_ms").over(w_recent_2))
)

metrics = (
    enriched
    .select(
        "*",
        F.stddev_samp("latency_ms_percent_rank").over(w3).alias("service_stddev_samp_latency_ms_percent_rank"),
        F.count("k_stddev_samp_latency_ms").over(w4).alias("service_count_k_stddev_samp_latency_ms"),
        F.cume_dist().over(w5).alias("latency_ms_percent_rank_cume_dist"),
    )
)

cleaned = (
    metrics
    .filter((F.col("log_ts") >= F.lit("2025-06-01 10:00:00").cast("timestamp")) & F.col("k").isNotNull())
    .select(
        "*",
        (F.col("service_count_k_stddev_samp_latency_ms") + F.col("service_stddev_samp_latency_ms_percent_rank")).alias("service_count_k_stddev_samp_latency_ms_plus_service_stddev_samp_latency_ms_percent_rank"),
    )
)

result = (
    cleaned
    .groupBy(F.date_trunc("DAY", "log_ts").alias("activity_day"))
    .agg(
        F.sum("service_stddev_samp_latency_ms_percent_rank").alias("total_service_stddev_samp_latency_ms_percent_rank"),
        F.round(F.avg("k_stddev_samp_latency_ms"), 2).alias("avg_k_stddev_samp_latency_ms"),
        F.count(F.when(F.col("level") == "DEBUG", 1)).alias("debug_rows"),
    )
    .filter(F.col("total_service_stddev_samp_latency_ms_percent_rank") < 0)
    .orderBy(F.col("activity_day").asc_nulls_last())
)
