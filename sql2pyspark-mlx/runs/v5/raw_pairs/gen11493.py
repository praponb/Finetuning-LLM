from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

base = (
    exploded
    .filter(F.to_date("log_ts") < "2023-09-01")
    .select(
        "id",
        "service",
        "latency_ms",
        "key",
        "attr_value",
        F.initcap(F.trim("key")).alias("key_title"),
        F.coalesce("key", F.lit("N/A")).alias("key_filled"),
    )
)

enriched = (
    base
    .filter(F.col("service") == "search")
    .withColumn("service_stddev_samp_latency_ms", F.stddev_samp("latency_ms").over(w))
)

cleaned = (
    enriched
    .filter((~F.col("key").eqNullSafe(F.lit("region"))) & (F.col("latency_ms") == 983))
    .select(
        "id",
        "service",
        "key",
        "key_title",
        "key_filled",
        "service_stddev_samp_latency_ms",
        F.when(F.col("key") == "trace", 1).otherwise(0).alias("is_trace"),
    )
)

daily = (
    cleaned
    .groupBy("key")
    .agg(
        F.count(F.when(F.col("service") == "auth", 1)).alias("auth_rows"),
        F.sum(F.when(F.col("key") == "version", F.col("service_stddev_samp_latency_ms"))).alias("version_total_service_stddev_samp_latency_ms"),
        F.countDistinct("service").alias("distinct_services"),
    )
)

result = (
    daily
    .select(
        "key",
        "auth_rows",
        "version_total_service_stddev_samp_latency_ms",
        "distinct_services",
        F.when(F.col("key").rlike("^(trace|region)$"), "GROUP_A").when(F.col("key").rlike("^(version)$"), "PRIORITY").otherwise("EXTENDED").alias("key_zone"),
    )
    .orderBy("key_zone")
)
