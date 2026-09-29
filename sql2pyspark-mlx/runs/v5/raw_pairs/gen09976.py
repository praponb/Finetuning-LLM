from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service")
w2 = Window.partitionBy("service").orderBy("service_count_status_code")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

filtered = (
    flattened
    .filter(F.col("log_ts") >= F.to_timestamp(F.lit("2025-06-01 08:00:00")))
    .select(
        "*",
        F.concat("attr_key", F.lit("/"), "service").alias("attr_key_service"),
        F.upper("attr_key").alias("attr_key_upper"),
    )
)

filtered_app_logs = (
    filtered
    .filter(F.col("status_code") < 376)
    .select(
        "service",
        "log_ts",
        "latency_ms",
        "status_code",
        "v",
        "attr_key_service",
        "attr_key_upper",
        F.when(F.col("service") == "search", 1).otherwise(0).alias("is_search"),
        F.trim("attr_key").alias("clean_attr_key"),
    )
)

with_windows = (
    filtered_app_logs
    .select(
        "service",
        "latency_ms",
        "attr_key_service",
        "is_search",
        "clean_attr_key",
        F.count("status_code").over(w).alias("service_count_status_code"),
    )
)

metrics = (
    with_windows
    .filter(F.col("service_count_status_code") == 10)
    .withColumn("latency_ms_share", F.round((F.col("latency_ms") / F.sum("latency_ms").over(w)), 4))
    .withColumn("service_count_status_code_cume_dist", F.cume_dist().over(w2))
)

result = (
    metrics
    .groupBy(
        F.when(F.col("latency_ms") >= 2276, "PREMIUM").otherwise("STANDARD").alias("latency_ms_tier"),
    )
    .agg(
        F.count_if(F.col("service") == "auth").alias("auth_events"),
        F.countDistinct("service").alias("distinct_services"),
    )
    .filter(F.col("distinct_services") < 1000)
)
