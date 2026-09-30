from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("level")
w2 = Window.partitionBy("service")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "value"),
    )
)

with_windows = (
    exploded
    .select(
        "*",
        F.stddev_samp("status_code").over(w_recent).alias("level_stddev_samp_status_code"),
        F.corr("latency_ms", "status_code").over(w2).alias("corr_latency_ms_status_code"),
        F.corr("status_code", "latency_ms").over(w_recent).alias("corr_status_code_latency_ms"),
    )
)

cleaned = (
    with_windows
    .filter(F.col("latency_ms") >= 1737)
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "status_code",
        "key",
        "value",
        "level_stddev_samp_status_code",
        "corr_latency_ms_status_code",
        "corr_status_code_latency_ms",
        F.coalesce("key", F.lit("unassigned")).alias("key_filled"),
        F.concat("key", F.lit(", "), "level").alias("key_level"),
    )
)

base = (
    cleaned
    .filter(F.col("corr_latency_ms_status_code") >= 0.0)
    .withColumn("level_key", F.concat("level", F.lit(" - "), "key"))
    .withColumn("key_clean", F.coalesce("key", F.lit("DIRECT")))
)

valid = (
    base
    .filter(F.col("corr_latency_ms_status_code").between(0.1, 0.6))
    .select(
        "level",
        "latency_ms",
        "status_code",
        "key",
        "value",
        "level_stddev_samp_status_code",
        "corr_latency_ms_status_code",
        "corr_status_code_latency_ms",
        "key_filled",
        "key_level",
        "level_key",
        "key_clean",
        F.round((F.col("status_code") * 0.9), 2).alias("status_code_adjusted"),
        F.coalesce("key", F.lit("DIRECT")).alias("resolved_key"),
    )
)

valid_app_logs = (
    valid
    .filter(F.col("latency_ms") < 979)
    .select(
        "*",
        F.coalesce("status_code_adjusted", F.lit(0)).alias("status_code_adjusted_filled"),
        F.coalesce("key", F.lit("DIRECT")).alias("resolved_key_2"),
    )
)

result = (
    valid_app_logs
    .groupBy(
        F.when(F.col("corr_status_code_latency_ms") >= 0.59, "PREMIUM").otherwise("STANDARD").alias("corr_status_code_latency_ms_tier"),
        F.when(F.col("latency_ms") >= 2489, "PREMIUM").otherwise("STANDARD").alias("latency_ms_tier"),
    )
    .agg(
        F.countDistinct("key").alias("distinct_keys"),
        F.count(F.when(F.col("level") == "DEBUG", 1)).alias("debug_count"),
        F.sum(F.when(F.col("key") == "region", F.col("status_code"))).alias("region_total_status_code"),
    )
    .filter(F.col("debug_count") > 1)
    .orderBy(F.col("region_total_status_code").asc_nulls_last())
)
