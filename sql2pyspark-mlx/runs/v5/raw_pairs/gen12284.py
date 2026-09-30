from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("key")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

unpacked = (
    lines
    .filter(F.col("key").isNotNull())
    .select(
        "latency_ms",
        "status_code",
        "key",
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("has_fault"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

enriched = (
    unpacked
    .filter(F.col("key").isin(["trace", "version"]))
    .select(
        "status_code",
        "key",
        F.stddev_samp("status_code_high").over(w_running).alias("key_stddev_samp_status_code_high"),
        F.sum("status_code_high").over(w_running).alias("key_sum_status_code_high"),
    )
)

enriched_app_logs = (
    enriched
    .select(
        "key",
        "key_stddev_samp_status_code_high",
        F.avg("key_sum_status_code_high").over(w_running).alias("key_avg_key_sum_status_code_high"),
        F.avg("key_sum_status_code_high").over(w_running).alias("key_avg_key_sum_status_code_high_2"),
    )
)

filtered = (
    enriched_app_logs
    .filter(F.col("key").isNotNull())
    .select(
        "*",
        F.coalesce("key_avg_key_sum_status_code_high_2", F.lit(0)).alias("key_avg_key_sum_status_code_high_2_filled"),
    )
)

result = (
    filtered
    .select(
        "key_stddev_samp_status_code_high",
        "key_avg_key_sum_status_code_high",
        "key_avg_key_sum_status_code_high_2",
        "key_avg_key_sum_status_code_high_2_filled",
    )
)
