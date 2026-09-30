from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host", "key")
w2 = Window.partitionBy("host").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w3 = Window.partitionBy("host")
w4 = Window.partitionBy("key")
w5 = Window.partitionBy("host").orderBy(F.col("host_min_latency_ms").asc_nulls_last())

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "v"),
    )
)

enriched = (
    exploded
    .withColumn("status_code_share", F.round((F.col("status_code") / F.sum("status_code").over(w)), 4))
    .withColumn("latency_ms_count_15m", F.count("latency_ms").over(w2))
    .withColumn("host_min_latency_ms", F.min("latency_ms").over(w3))
    .withColumn("status_code_share_2", F.round((F.col("status_code") / F.sum("status_code").over(w3)), 4))
)

scored = (
    enriched
    .select(
        "id",
        "host",
        "log_ts",
        "status_code",
        "v",
        "status_code_share",
        "host_min_latency_ms",
        "status_code_share_2",
        F.stddev_samp("status_code").over(w4).alias("key_stddev_samp_status_code"),
    )
)

metrics = (
    scored
    .select(
        "host",
        "log_ts",
        "v",
        "host_min_latency_ms",
        "status_code_share_2",
        "key_stddev_samp_status_code",
        F.avg("key_stddev_samp_status_code").over(w3).alias("host_avg_key_stddev_samp_status_code"),
        F.rank().over(w5).alias("host_min_latency_ms_rank"),
    )
)

valid = (
    metrics
    .filter(F.col("log_ts") > F.to_timestamp(F.lit("2025-06-01 09:00:00")))
    .withColumn("status_code_share_2_minus_key_stddev_samp_status_code", (F.col("status_code_share_2") - F.col("key_stddev_samp_status_code")))
    .withColumn("host_min_latency_ms_rank_str", F.col("host_min_latency_ms_rank").cast("double"))
)

result = (
    valid
    .select(
        "host",
        "status_code_share_2",
        "key_stddev_samp_status_code",
        "host_avg_key_stddev_samp_status_code",
        "host_min_latency_ms_rank",
        "status_code_share_2_minus_key_stddev_samp_status_code",
        "host_min_latency_ms_rank_str",
    )
)
