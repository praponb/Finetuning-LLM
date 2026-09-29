from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host")
w2 = Window.orderBy(F.col("latency_ms_rounded").desc_nulls_first())
w3 = Window.partitionBy("host").orderBy(F.col("k_length").asc_nulls_last())

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
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

cleaned = (
    exploded
    .filter(F.col("k").isNotNull() & (F.col("latency_ms") <= 696))
    .withColumn("latency_ms_rounded", F.round("latency_ms", 2))
    .withColumn("k_clean", F.coalesce("k", F.lit("DIRECT")))
)

valid = (
    cleaned
    .filter((F.col("log_ts") >= F.lit("2025-06-01").cast("date")) & F.col("k").isNotNull())
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "status_code",
        "attr_value",
        "latency_ms_rounded",
        "k_clean",
        F.length("k").alias("k_length"),
    )
)

enriched = (
    valid
    .filter(F.col("service").isin(["auth", "search", "billing"]))
    .select(
        "id",
        "host",
        "status_code",
        "latency_ms_rounded",
        "k_clean",
        "k_length",
        F.max("latency_ms_rounded").over(w).alias("host_max_latency_ms_rounded"),
        F.dense_rank().over(w2).alias("latency_ms_rounded_dense_rank"),
        F.round((F.col("status_code") / F.sum("status_code").over(w)), 4).alias("status_code_share"),
        F.percent_rank().over(w3).alias("k_length_percent_rank"),
    )
)

result = (
    enriched
    .select(
        "id",
        "status_code",
        "latency_ms_rounded",
        "k_length",
        "latency_ms_rounded_dense_rank",
        "k_length_percent_rank",
    )
    .orderBy(F.col("k_length_percent_rank").desc_nulls_last(), "id")
)
