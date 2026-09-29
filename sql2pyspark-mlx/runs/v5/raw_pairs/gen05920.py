from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("key", "v"),
    )
)

filtered = (
    flattened
    .filter(~F.col("key").eqNullSafe(F.lit("version")))
    .select(
        "service",
        "host",
        "log_ts",
        "level",
        "key",
        "v",
        F.upper("level").alias("level_upper"),
        F.substring("host", 1, 2).alias("host_prefix"),
    )
)

cleaned = (
    filtered
    .filter((F.col("service") == "auth") & (F.col("log_ts") > F.lit("2025-06-01 14:00:00").cast("timestamp")))
    .select(
        "*",
        F.concat_ws("-", "service", "key").alias("service_key_key"),
        F.coalesce("key", F.lit("Unknown")).alias("key_filled"),
    )
)

valid = (
    cleaned
    .filter((~F.col("key").eqNullSafe(F.lit("trace"))) & F.col("key").isNotNull())
    .select("*", F.length("host").alias("host_length"))
)

enriched = (
    valid
    .filter(F.col("host_length") <= 39)
    .select("*", F.avg("host_length").over(w).alias("level_avg_host_length"))
)

stats = (
    enriched
    .cube("level")
    .agg(
        F.sum(F.when(F.col("level") == "WARN", F.col("level_avg_host_length")).otherwise(0)).alias("warn_level_avg_host_length"),
        F.min("log_ts").alias("first_log_ts"),
        F.grouping("level").alias("grouping_level"),
    )
)

result = (
    stats
    .select("warn_level_avg_host_length", "first_log_ts")
    .orderBy(F.col("warn_level_avg_host_length").desc_nulls_first())
)
