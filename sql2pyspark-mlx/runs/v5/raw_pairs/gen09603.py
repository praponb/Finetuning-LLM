from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy(F.col("latency_ms_per_log_ts_hour_filled").desc_nulls_first())
w_ordered = Window.partitionBy("service")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

base = (
    flattened
    .filter((F.col("log_ts") >= F.to_timestamp(F.lit("2025-06-01 11:00:00"))) & (F.col("log_ts") < "2025-07-10"))
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        "key",
        "attr_value",
        F.round("status_code", 0).alias("status_code_rounded"),
        F.hour("log_ts").alias("log_ts_hour"),
        F.coalesce("key", F.lit("N/A")).alias("key_clean"),
    )
)

valid = (
    base
    .filter(F.col("service") == "auth")
    .withColumn("latency_ms_per_log_ts_hour", (F.col("latency_ms") / F.nullif("log_ts_hour", F.lit(0))))
    .withColumn("key_filled", F.coalesce("key", F.lit("UNKNOWN")))
)

filtered = (
    valid
    .filter(F.col("key").isNotNull())
    .select(
        "id",
        "service",
        "latency_ms",
        "key",
        "status_code_rounded",
        "log_ts_hour",
        "latency_ms_per_log_ts_hour",
        "key_filled",
        F.upper("host").alias("host_upper"),
    )
)

base_app_logs = (
    filtered
    .filter(F.col("key").isNotNull() & (F.col("latency_ms_per_log_ts_hour") > 75))
    .select(
        "service",
        "status_code_rounded",
        "latency_ms_per_log_ts_hour",
        "key_filled",
        F.coalesce("latency_ms_per_log_ts_hour", F.lit(0)).alias("latency_ms_per_log_ts_hour_filled"),
        F.concat_ws("-", "service", "key").alias("service_key_key"),
        F.coalesce("key", F.lit("NONE")).alias("key_filled_2"),
    )
)

metrics = (
    base_app_logs
    .select(
        "service",
        "latency_ms_per_log_ts_hour",
        "key_filled",
        "service_key_key",
        F.rank().over(w).alias("latency_ms_per_log_ts_hour_filled_rank"),
    )
)

enriched = (
    metrics
    .filter(F.col("latency_ms_per_log_ts_hour_filled_rank") <= 7)
    .select(
        "service",
        "key_filled",
        "service_key_key",
        "latency_ms_per_log_ts_hour_filled_rank",
        F.sum("latency_ms_per_log_ts_hour_filled_rank").over(w_ordered).alias("service_sum_latency_ms_per_log_ts_hour_filled_rank"),
        F.sum("latency_ms_per_log_ts_hour").over(w_ordered).alias("service_sum_latency_ms_per_log_ts_hour"),
    )
)

result = (
    enriched
    .select(
        "service",
        "key_filled",
        "latency_ms_per_log_ts_hour_filled_rank",
        "service_sum_latency_ms_per_log_ts_hour",
        F.when(F.col("service").rlike("^(auth|billing)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("service_zone"),
    )
    .orderBy(F.col("latency_ms_per_log_ts_hour_filled_rank").asc_nulls_first())
)
