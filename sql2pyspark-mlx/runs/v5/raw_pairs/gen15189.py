from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level")
w2 = Window.partitionBy("host")
w3 = Window.partitionBy("host").orderBy(F.col("host_min_latency_ms_whole").desc_nulls_first())
w4 = Window.partitionBy("host").orderBy(F.col("host_min_latency_ms_whole").desc_nulls_last())

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("k", "v"),
    )
)

base = (
    exploded
    .filter(F.col("k") == "trace")
    .select(
        "*",
        F.ceil("latency_ms").alias("latency_ms_whole"),
        F.coalesce("latency_ms", F.lit(0)).alias("latency_ms_filled"),
        F.coalesce("k", F.lit("UNKNOWN")).alias("k_clean"),
    )
)

enriched = (
    base
    .select(
        "id",
        "host",
        "latency_ms",
        "v",
        "latency_ms_whole",
        "k_clean",
        F.min("latency_ms_filled").over(w).alias("level_min_latency_ms_filled"),
    )
)

with_windows = (
    enriched
    .filter(F.col("latency_ms") == 1580)
    .select(
        "*",
        F.stddev_samp("latency_ms_whole").over(w2).alias("host_stddev_samp_latency_ms_whole"),
    )
)

with_windows_app_logs = (
    with_windows
    .withColumn("host_min_latency_ms_whole", F.min("latency_ms_whole").over(w2))
)

valid = (
    with_windows_app_logs
    .filter(F.col("host_min_latency_ms_whole") != 34)
    .withColumn("latency_ms_whole_filled", F.coalesce("latency_ms_whole", F.lit(0)))
    .withColumn("host_min_latency_ms_whole_minus_host_stddev_samp_latency_ms_whole", (F.col("host_min_latency_ms_whole") - F.col("host_stddev_samp_latency_ms_whole")))
)

enriched_app_logs = (
    valid
    .select(
        "*",
        F.cume_dist().over(w3).alias("host_min_latency_ms_whole_cume_dist"),
        F.dense_rank().over(w4).alias("host_min_latency_ms_whole_dense_rank"),
    )
)

result = (
    enriched_app_logs
    .rollup(
        "host",
        F.when(F.col("host_stddev_samp_latency_ms_whole") >= 73, "HIGH").otherwise("LOW").alias("host_stddev_samp_latency_ms_whole_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.count_if(F.col("host") == "h3").alias("h3_events"),
        F.sum("host_min_latency_ms_whole_minus_host_stddev_samp_latency_ms_whole").alias("total_host_min_latency_ms_whole_minus_host_stddev_samp_latency_ms_whole"),
    )
    .filter(F.col("row_count") < 1000)
)
