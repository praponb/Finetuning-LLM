from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status_code_rounded_tier").orderBy(F.col("row_count").desc())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "v"),
    )
)

unpacked = (
    exploded
    .filter(F.col("log_ts") < "2024-07-18")
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        "key",
        "v",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("has_fault"),
    )
)

base = (
    unpacked
    .filter((F.col("log_ts") >= F.lit("2025-06-01").cast("date")) & (F.col("latency_ms") != 2039))
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "status_code",
        "key",
        "v",
        F.hour("log_ts").alias("log_ts_hour"),
    )
)

filtered = (
    base
    .filter(F.col("log_ts_hour") != 80)
    .withColumn("status_code_rounded", F.round("status_code", 0))
)

result = (
    filtered
    .groupBy(
        F.when(F.col("status_code_rounded") >= 72, "HIGH").otherwise("LOW").alias("status_code_rounded_tier"),
        "key",
    )
    .agg(
        F.sort_array(F.collect_set("key")).alias("key_list"),
        F.count("*").alias("row_count"),
        F.round(F.sum("status_code_rounded") / F.nullif(F.count(F.when(F.col("key") == "region", 1)), F.lit(0)), 2).alias("status_code_rounded_per_region"),
    )
    .withColumn("row_count_rank", F.rank().over(w))
)
